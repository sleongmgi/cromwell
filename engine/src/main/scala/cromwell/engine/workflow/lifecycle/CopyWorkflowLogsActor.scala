package cromwell.engine.workflow.lifecycle

import java.nio.file.Path

import akka.actor.Props
import cromwell.core.{PathCopier, WorkflowId}
import cromwell.engine.EngineWorkflowDescriptor
import cromwell.engine.workflow.lifecycle.execution.{ExecutionStore, OutputStore}

object CopyWorkflowLogsActor {
  def props(workflowId: WorkflowId, workflowDescriptor: EngineWorkflowDescriptor, executionStore: ExecutionStore, outputStore: OutputStore) = Props(
    new CopyWorkflowLogsActor(workflowId, workflowDescriptor, executionStore, outputStore)
  )
}

class CopyWorkflowLogsActor(workflowId: WorkflowId, val workflowDescriptor: EngineWorkflowDescriptor,
                            executionStore: ExecutionStore, outputStore: OutputStore)
  extends EngineWorkflowCopyFinalizationActor {

  override def copyFiles(): Unit = {
    for {
      workflowLogDirString <- getWorkflowOption("workflow_log_dir")
      tempLogFilePath <- getTempLogFilePathOption
    } yield copyWorkflowLog(tempLogFilePath, workflowLogDirString)
  }

  // TODO: PBE: Where are the workflow logs?
  private def getTempLogFilePathOption: Option[Path] = ???

  private def copyWorkflowLog(tempLogFilePath: Path, workflowLogDirString: String): Unit = {
    val workflowLogDirPath = convertStringToPath(workflowLogDirString)
    val destinationFilePath = workflowLogDirPath.resolve(tempLogFilePath.getFileName.toString)
    PathCopier.copy(tempLogFilePath, destinationFilePath)
  }
}
