import React from "react";
import { DataTable } from "../../components";
import _ from "lodash";

const taskDetailFields = [
  { name: "seq" },
  { name: "workflowTask.name", label: "Name" },
  { name: "referenceTaskName", label: "Task Reference" },
  { name: "workflowTask.type", label: "Type" },
  { name: "scheduledTime", type: "date" },
  { name: "startTime", type: "date" },
  { name: "endTime", type: "date" },
  { name: "status" },
  { name: "updateTime", type: "date" },
  { name: "callbackAfterSeconds" },
  { name: "pollCount" },
];

export default function TaskList({ selectedTask, tasks, dag, onClick }) {
  let selectedTaskIdx = -1;
  if (selectedTask) {
    const { ref, taskId } = selectedTask;
    if (taskId) {
      selectedTaskIdx = tasks.findIndex((t) => t.taskId === taskId);
    } else {
      selectedTaskIdx = _.findLastIndex(
        tasks,
        (t) => t.referenceTaskName === ref
      );
    }
  }

  if (selectedTaskIdx === -1) selectedTaskIdx = null;

  const handleClick = (currentRowsSelected, allRowsSelected, rowsSelected) => {
    if (!_.isEmpty(rowsSelected)) {
      if (onClick) {
        const task = tasks[rowsSelected[0]];
        const node = dag.graph.node(task.referenceTaskName);

        // If there are more than 1 task associated, use task ID
        if (node.taskResults.length > 1) {
          onClick({
            ref: task.referenceTaskName,
            taskId: task.taskId,
          });
        } else {
          onClick({
            ref: task.referenceTaskName,
          });
        }
      }
    } else {
      if (onClick) onClick(null);
    }
  };

  return (
    <DataTable
      style={{ minHeight: 400 }}
      data={tasks}
      columns={taskDetailFields}
      defaultShowColumns={[
        "seq",
        "workflowTask.name",
        "referenceTaskName",
        "workflowTask.type",
        "startTime",
        "endTime",
        "status",
      ]}
      localStorageKey="taskListTable"
      options={{
        selectableRows: "single",
        selectableRowsOnClick: true,
        selectableRowsHideCheckboxes: true,
        rowsSelected: [selectedTaskIdx],
        onRowSelectionChange: handleClick,
      }}
    />
  );
}
