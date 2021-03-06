package cromwell.logging

import java.util.UUID

import cromwell.CromwellTestkitSpec
import cromwell.core.WorkflowId
import cromwell.engine.WorkflowSourceFiles
import cromwell.engine.backend.local.LocalBackend
import cromwell.engine.backend.{BackendCallJobDescriptor, WorkflowDescriptorBuilder}
import cromwell.engine.workflow.BackendCallKey
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import wdl4s.values.WdlValue

class WorkflowLoggerSpec extends FlatSpec with Matchers with BeforeAndAfterAll with WorkflowDescriptorBuilder {
  val testWorkflowManagerSystem = new CromwellTestkitSpec.TestWorkflowManagerSystem()
  override implicit val actorSystem = testWorkflowManagerSystem.actorSystem

  override protected def afterAll() = {
    testWorkflowManagerSystem.shutdownTestActorSystem()
    super.afterAll()
  }

  val descriptor = materializeWorkflowDescriptorFromSources(id = WorkflowId(UUID.fromString("fc6cfad9-65e9-4eb7-853f-7e08c1c8cf8e")),
    workflowSources = WorkflowSourceFiles(
      "task x {command {ps}} workflow w {call x}",
      "{}",
      "{}"
    )
  )
  val backend = LocalBackend(testWorkflowManagerSystem.actorSystem)
  val jobDescriptor = BackendCallJobDescriptor(
    descriptor,
    BackendCallKey(descriptor.namespace.workflow.calls.find(_.unqualifiedName == "x").head, None, 1),
    Map.empty[String, WdlValue])

  "WorkflowLogger" should "create a valid tag" in {
    backend.workflowLogger(descriptor).tag shouldBe "LocalBackend [UUID(fc6cfad9)]"
  }

  it should "create a valid tag for backend call" in {
    backend.jobLogger(jobDescriptor).tag shouldBe "LocalBackend [UUID(fc6cfad9):x]"
  }
}
