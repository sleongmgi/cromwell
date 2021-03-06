package cromwell.webservice

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import akka.util.Timeout
import cromwell.core.WorkflowId
import cromwell.engine._
import cromwell.engine.backend.{Backend, CallLogs}
import cromwell.engine.workflow.MaterializeWorkflowDescriptorActor.{MaterializationFailure, MaterializationSuccess, MaterializeWorkflow}
import cromwell.engine.workflow.WorkflowManagerActor
import cromwell.engine.workflow.WorkflowManagerActor.{CallNotFoundException, WorkflowNotFoundException}
import cromwell.webservice.PerRequest.{RequestComplete, RequestCompleteWithHeaders}
import cromwell.{core, engine}
import spray.http.HttpHeaders.Link
import spray.http.{HttpHeader, StatusCodes, Uri}
import spray.httpx.SprayJsonSupport._

import scala.concurrent.duration._
import scala.language.postfixOps

object CromwellApiHandler {
  def props(requestHandlerActor: ActorRef): Props = {
    Props(new CromwellApiHandler(requestHandlerActor))
  }

  sealed trait ApiHandlerMessage

  final case class ApiHandlerWorkflowSubmit(source: WorkflowSourceFiles) extends ApiHandlerMessage
  final case class ApiHandlerWorkflowSubmitBatch(sources: Seq[WorkflowSourceFiles]) extends ApiHandlerMessage
  final case class ApiHandlerWorkflowQuery(uri: Uri, parameters: Seq[(String, String)]) extends ApiHandlerMessage
  final case class ApiHandlerWorkflowStatus(id: WorkflowId) extends ApiHandlerMessage
  final case class ApiHandlerWorkflowOutputs(id: WorkflowId) extends ApiHandlerMessage
  final case class ApiHandlerWorkflowAbort(id: WorkflowId) extends ApiHandlerMessage
  final case class ApiHandlerCallOutputs(id: WorkflowId, callFqn: String) extends ApiHandlerMessage
  final case class ApiHandlerCallStdoutStderr(id: WorkflowId, callFqn: String) extends ApiHandlerMessage
  final case class ApiHandlerWorkflowStdoutStderr(id: WorkflowId) extends ApiHandlerMessage
  final case class ApiHandlerCallCaching(id: WorkflowId, parameters: QueryParameters, callName: Option[String]) extends ApiHandlerMessage
  final case class ApiHandlerWorkflowMetadata(id: WorkflowId,
                                              parameters: WorkflowMetadataQueryParameters) extends ApiHandlerMessage
  final case class ApiHandlerValidateWorkflow(id: WorkflowId, wdlSource: String, workflowInputs: Option[String], workflowOptions: Option[String]) extends ApiHandlerMessage

  sealed trait WorkflowManagerResponse

  sealed trait WorkflowManagerSuccessResponse extends WorkflowManagerResponse

  sealed trait WorkflowManagerFailureResponse extends WorkflowManagerResponse {
    def failure: Throwable
  }

  final case class WorkflowManagerSubmitSuccess(id: WorkflowId) extends WorkflowManagerSuccessResponse
  final case class WorkflowManagerSubmitFailure(override val failure: Throwable) extends WorkflowManagerFailureResponse
  final case class WorkflowManagerWorkflowOutputsSuccess(id: WorkflowId, outputs: engine.WorkflowOutputs) extends WorkflowManagerSuccessResponse
  final case class WorkflowManagerWorkflowOutputsFailure(id: WorkflowId, override val failure: Throwable) extends WorkflowManagerFailureResponse
  final case class WorkflowManagerStatusSuccess(id: WorkflowId, state: WorkflowState) extends WorkflowManagerSuccessResponse
  final case class WorkflowManagerStatusFailure(id: WorkflowId, override val failure: Throwable) extends WorkflowManagerFailureResponse
  final case class WorkflowManagerAbortSuccess(id: WorkflowId) extends WorkflowManagerSuccessResponse
  final case class WorkflowManagerAbortFailure(id: WorkflowId, override val failure: Throwable) extends WorkflowManagerFailureResponse
  final case class WorkflowManagerQuerySuccess(uri: Uri, response: WorkflowQueryResponse, metadata: Option[QueryMetadata]) extends WorkflowManagerSuccessResponse
  final case class WorkflowManagerQueryFailure(override val failure: Throwable) extends WorkflowManagerFailureResponse
  final case class WorkflowManagerCallOutputsSuccess(id: WorkflowId, callFqn: FullyQualifiedName, outputs: core.CallOutputs) extends WorkflowManagerSuccessResponse
  final case class WorkflowManagerCallOutputsFailure(id: WorkflowId, callFqn: FullyQualifiedName, override val failure: Throwable) extends WorkflowManagerFailureResponse
  final case class WorkflowManagerCallStdoutStderrSuccess(id: WorkflowId, callFqn: FullyQualifiedName, logs: Seq[CallLogs]) extends WorkflowManagerSuccessResponse
  final case class WorkflowManagerCallStdoutStderrFailure(id: WorkflowId, callFqn: FullyQualifiedName, override val failure: Throwable) extends WorkflowManagerFailureResponse
  final case class WorkflowManagerWorkflowStdoutStderrSuccess(id: WorkflowId, logs: Map[FullyQualifiedName, Seq[CallLogs]]) extends WorkflowManagerSuccessResponse
  final case class WorkflowManagerWorkflowStdoutStderrFailure(id: WorkflowId, override val failure: Throwable) extends WorkflowManagerFailureResponse
  final case class WorkflowManagerWorkflowMetadataSuccess(id: WorkflowId, response: WorkflowMetadataResponse) extends WorkflowManagerSuccessResponse
  final case class WorkflowManagerWorkflowMetadataFailure(id: WorkflowId, override val failure: Throwable) extends WorkflowManagerFailureResponse
  final case class WorkflowManagerCallCachingSuccess(id: WorkflowId, updateCount: Int) extends WorkflowManagerSuccessResponse
  final case class WorkflowManagerCallCachingFailure(id: WorkflowId, override val failure: Throwable) extends WorkflowManagerFailureResponse
  final case class WorkflowManagerBatchSubmitResponse(responses: Seq[WorkflowManagerResponse]) extends WorkflowManagerResponse
}

class CromwellApiHandler(requestHandlerActor: ActorRef) extends Actor {
  import CromwellApiHandler._
  import WorkflowJsonSupport._

  implicit val timeout = Timeout(2 seconds)
  val log = Logging(context.system, classOf[CromwellApiHandler])

  def workflowNotFound(id: WorkflowId) = RequestComplete(StatusCodes.NotFound, APIResponse.error(new Throwable(s"Workflow '$id' not found.")))
  def callNotFound(callFqn: String, id: WorkflowId) = {
    RequestComplete(StatusCodes.NotFound, APIResponse.error(new Throwable(s"Call $callFqn not found for workflow '$id'.")))
  }

  private def error(t: Throwable)(f: Throwable => RequestComplete[_]): Unit = context.parent ! f(t)

  private def generatePaginationParams(page: Int, pageSize: Int): String = {
    s"page=$page&pagesize=$pageSize"
  }

  //Generates link headers for pagination navigation https://tools.ietf.org/html/rfc5988#page-6
  private def generateLinkHeaders(uri: Uri, metadata: Option[QueryMetadata]): Seq[HttpHeader] = {
    //strip off the query params
    val baseUrl = uri.scheme + ":" + uri.authority + uri.path
    metadata match {
      case Some(meta) =>
        (meta.page, meta.pageSize) match {
          case (Some(p), Some(ps)) =>

            val firstLink = Link(Uri(baseUrl).withQuery(generatePaginationParams(1, ps)), Link.first)

            val prevPage = math.max(p - 1, 1)
            val prevLink = Link(Uri(baseUrl).withQuery(generatePaginationParams(prevPage, ps)), Link.prev)

            val lastPage = math.ceil(meta.totalRecords.getOrElse(1).toDouble / ps.toDouble).toInt
            val lastLink = Link(Uri(baseUrl).withQuery(generatePaginationParams(lastPage, ps)), Link.last)

            val nextPage = math.min(p + 1, lastPage)
            val nextLink = Link(Uri(baseUrl).withQuery(generatePaginationParams(nextPage, ps)), Link.next)

            Seq(firstLink, prevLink, nextLink, lastLink)

          case _ => Seq()
        }
      case None => Seq()
    }
  }

  override def receive = {
    case ApiHandlerWorkflowStatus(id) => requestHandlerActor ! WorkflowManagerActor.WorkflowStatus(id)
    case WorkflowManagerStatusSuccess(id, state) => context.parent ! RequestComplete(StatusCodes.OK, WorkflowStatusResponse(id.toString, state.toString))
    case WorkflowManagerStatusFailure(_, e) =>
      error(e) {
        case _: WorkflowNotFoundException => RequestComplete(StatusCodes.NotFound, e)
        case _ => RequestComplete(StatusCodes.InternalServerError, e)
      }

    case ApiHandlerWorkflowQuery(uri, parameters) => requestHandlerActor ! WorkflowManagerActor.WorkflowQuery(uri, parameters)
    case WorkflowManagerQuerySuccess(uri, response, metadata) =>
      context.parent ! RequestCompleteWithHeaders(response, generateLinkHeaders(uri, metadata):_*)
    case WorkflowManagerQueryFailure(e) =>
      error(e) {
        case _: IllegalArgumentException => RequestComplete(StatusCodes.BadRequest, APIResponse.fail(e))
        case _ => RequestComplete(StatusCodes.InternalServerError, APIResponse.error(e))
      }

    case ApiHandlerWorkflowAbort(id) => requestHandlerActor ! WorkflowManagerActor.WorkflowAbort(id)
    case WorkflowManagerAbortSuccess(id) =>
      context.parent ! RequestComplete(StatusCodes.OK, WorkflowAbortResponse(id.toString, WorkflowAborted.toString))
    case WorkflowManagerAbortFailure(_, e) =>
      error(e) {
        case _: WorkflowNotFoundException => RequestComplete(StatusCodes.NotFound, APIResponse.error(e))
        case _: IllegalStateException => RequestComplete(StatusCodes.Forbidden, APIResponse.error(e))
        case _ => RequestComplete(StatusCodes.InternalServerError, APIResponse.error(e))
      }

    case ApiHandlerWorkflowSubmit(source) => requestHandlerActor ! WorkflowManagerActor.SubmitWorkflow(source)
    case WorkflowManagerSubmitSuccess(id) =>
      context.parent ! RequestComplete(StatusCodes.Created, WorkflowSubmitResponse(id.toString, engine.WorkflowSubmitted.toString))
    case WorkflowManagerSubmitFailure(e) =>
      error(e) {
        case _: IllegalArgumentException => RequestComplete(StatusCodes.BadRequest, APIResponse.fail(e))
        case _ => RequestComplete(StatusCodes.InternalServerError, APIResponse.error(e))
      }

    case ApiHandlerWorkflowSubmitBatch(sources) =>
      context.actorOf(
        Props(new WorkflowSubmitBatchActor(self, requestHandlerActor, sources)),
        "WorkflowSubmitBatchActor")

    case WorkflowManagerBatchSubmitResponse(responses) =>
      val requestResponse: Seq[Either[WorkflowSubmitResponse, FailureResponse]] = responses.map {
        case WorkflowManagerSubmitSuccess(id) => Left(WorkflowSubmitResponse(id.toString, engine.WorkflowSubmitted.toString))
        case WorkflowManagerSubmitFailure(e) =>
          Right(e match {
            case _: IllegalArgumentException => APIResponse.fail(e)
            case _ => APIResponse.error(e)
          })
        case unexpected => Right(FailureResponse("error", s"unexpected message: $unexpected", None))
      }
      context.parent ! RequestComplete(StatusCodes.OK, requestResponse)


    case ApiHandlerWorkflowOutputs(id) => requestHandlerActor ! WorkflowManagerActor.WorkflowOutputs(id)
    case WorkflowManagerWorkflowOutputsSuccess(id, outputs) =>
      context.parent ! RequestComplete(StatusCodes.OK, WorkflowOutputResponse(id.toString, outputs.mapToValues))
    case WorkflowManagerWorkflowOutputsFailure(id, e) =>
      error(e) {
        case _: WorkflowNotFoundException => workflowNotFound(id)
        case _ => RequestComplete(StatusCodes.InternalServerError, APIResponse.error(e))
      }

    case ApiHandlerCallOutputs(id, callFqn) => requestHandlerActor ! WorkflowManagerActor.CallOutputs(id, callFqn)
    case WorkflowManagerCallOutputsSuccess(id, callFqn, outputs) =>
      context.parent ! RequestComplete(StatusCodes.OK, CallOutputResponse(id.toString, callFqn, outputs.mapToValues))
    case WorkflowManagerCallOutputsFailure(id, callFqn, e) =>
      error(e) {
        case _: WorkflowNotFoundException => workflowNotFound(id)
        case _: CallNotFoundException => callNotFound(callFqn, id)
        case _ => RequestComplete(StatusCodes.InternalServerError, APIResponse.error(e))
      }

    case ApiHandlerCallStdoutStderr(id, callFqn) => requestHandlerActor ! WorkflowManagerActor.CallStdoutStderr(id, callFqn)
    case WorkflowManagerCallStdoutStderrSuccess(id, callFqn, logs) =>
      context.parent ! RequestComplete(StatusCodes.OK, CallStdoutStderrResponse(id.toString, Map(callFqn -> logs)))
    case WorkflowManagerCallStdoutStderrFailure(id, callFqn, e) =>
      error(e) {
        case _: WorkflowNotFoundException => workflowNotFound(id)
        case _: CallNotFoundException => callNotFound(callFqn, id)
        case _: Backend.StdoutStderrException => RequestComplete(StatusCodes.InternalServerError, APIResponse.error(e))
        case _ => RequestComplete(StatusCodes.InternalServerError, APIResponse.error(e))
      }

    case ApiHandlerWorkflowStdoutStderr(id) => requestHandlerActor ! WorkflowManagerActor.WorkflowStdoutStderr(id)
    case WorkflowManagerWorkflowStdoutStderrSuccess(id, callLogs) =>
          context.parent ! RequestComplete(StatusCodes.OK, CallStdoutStderrResponse(id.toString, callLogs))
    case WorkflowManagerWorkflowStdoutStderrFailure(id, e) =>
      error(e) {
        case _: WorkflowNotFoundException => workflowNotFound(id)
        case _ => RequestComplete(StatusCodes.InternalServerError, APIResponse.error(e))
      }

    case ApiHandlerWorkflowMetadata(id, parameters) =>
      requestHandlerActor ! WorkflowManagerActor.WorkflowMetadata(id, parameters)
    case WorkflowManagerWorkflowMetadataSuccess(id, response) => context.parent ! RequestComplete(StatusCodes.OK, response)
    case WorkflowManagerWorkflowMetadataFailure(id, e) =>
      error(e) {
        case _: WorkflowNotFoundException => workflowNotFound(id)
        case _ => RequestComplete(StatusCodes.InternalServerError, APIResponse.error(e))
      }

    case ApiHandlerCallCaching(id, parameters, callName) => requestHandlerActor ! WorkflowManagerActor.CallCaching(id, parameters, callName)
    case WorkflowManagerCallCachingSuccess(id, updateCount) => context.parent ! RequestComplete(StatusCodes.OK, CallCachingResponse(updateCount))
    case WorkflowManagerCallCachingFailure(id, e) =>
      error(e) {
        case _: WorkflowNotFoundException => workflowNotFound(id)
        case _: IllegalArgumentException => RequestComplete(StatusCodes.BadRequest, APIResponse.fail(e))
        case _ => RequestComplete(StatusCodes.InternalServerError, APIResponse.error(e))
      }

    case ApiHandlerValidateWorkflow(id, wdlSource, workflowInputs, workflowOptions) =>
      requestHandlerActor ! MaterializeWorkflow(id, WorkflowSourceFiles(wdlSource, workflowInputs.getOrElse("{ }"), workflowOptions.getOrElse("{ }")))
    case MaterializationSuccess(_) => context.parent ! RequestComplete(StatusCodes.OK, APIResponse.success("Validation succeeded."))
    case MaterializationFailure(reason) => context.parent ! RequestComplete(StatusCodes.BadRequest,APIResponse.fail(reason))
  }
}
