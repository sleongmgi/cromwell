package cromwell.backend.impl.jes

import akka.actor.Props
import cromwell.backend.impl.jes.io._
import cromwell.backend.{BackendConfigurationDescriptor, BackendExecutionStore, BackendJobDescriptor, BackendJobDescriptorKey, BackendLifecycleActorFactory, BackendOutputStore, BackendWorkflowDescriptor}
import wdl4s.Call
import wdl4s.expression.WdlStandardLibraryFunctions

case class JesBackendLifecycleActorFactory(configurationDescriptor: BackendConfigurationDescriptor) extends BackendLifecycleActorFactory {

  val jesConfiguration = new JesConfiguration(configurationDescriptor)

  override def workflowInitializationActorProps(workflowDescriptor: BackendWorkflowDescriptor,
                                                calls: Seq[Call]): Option[Props] = {
    Option(JesInitializationActor.props(workflowDescriptor, calls, jesConfiguration))
  }

  override def jobExecutionActorProps(jobDescriptor: BackendJobDescriptor): Props = {
    JesJobExecutionActor.props(jobDescriptor, jesConfiguration)
  }

  override def workflowFinalizationActorProps(workflowDescriptor: BackendWorkflowDescriptor,
                                              calls: Seq[Call],
                                              executionStore: BackendExecutionStore,
                                              outputStore: BackendOutputStore) = {
    Option(JesFinalizationActor.props(workflowDescriptor, calls, jesConfiguration, executionStore, outputStore))
  }

  override def expressionLanguageFunctions(workflowDescriptor: BackendWorkflowDescriptor,
                                           jobKey: BackendJobDescriptorKey): WdlStandardLibraryFunctions = {

    val fileSystem = buildFilesystem(workflowDescriptor, jesConfiguration.jesAttributes.gcsFilesystemAuth, jesConfiguration.googleConfig)
    val jesCallPaths = JesCallPaths(jobKey, workflowDescriptor, jesConfiguration)
    new JesExpressionFunctions(List(fileSystem), jesCallPaths.callContext)
  }

  override def getExecutionRootPath(workflowDescriptor: BackendWorkflowDescriptor) = {
    Option(JesWorkflowPaths(workflowDescriptor, jesConfiguration).rootPath)
  }
}
