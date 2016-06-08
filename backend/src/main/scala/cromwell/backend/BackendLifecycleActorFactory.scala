package cromwell.backend

import java.nio.file.Path

import akka.actor.Props
import cromwell.core.{ExecutionStore, OutputStore}
import wdl4s.Call
import wdl4s.expression.WdlStandardLibraryFunctions

trait BackendLifecycleActorFactory {
  def workflowInitializationActorProps(workflowDescriptor: BackendWorkflowDescriptor,
                                       calls: Seq[Call]): Option[Props]

  def jobExecutionActorProps(jobDescriptor: BackendJobDescriptor): Props

  def workflowFinalizationActorProps(workflowDescriptor: BackendWorkflowDescriptor,
                                     calls: Seq[Call],
                                     executionStore: ExecutionStore,
                                     outputStore: OutputStore): Option[Props] = None

  def expressionLanguageFunctions(workflowDescriptor: BackendWorkflowDescriptor,
                                  jobKey: BackendJobDescriptorKey): WdlStandardLibraryFunctions

  def getExecutionRootPath(workflowDescriptor: BackendWorkflowDescriptor): Option[Path] = None
}
