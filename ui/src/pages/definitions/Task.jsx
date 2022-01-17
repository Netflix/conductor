import React from "react";
import { NavLink, DataTable } from "../../components";
import { makeStyles } from "@material-ui/styles";
import { useFetch } from "../../utils/query";
import Header from "./Header";
import sharedStyles from "../styles";
import { Helmet } from "react-helmet";

const useStyles = makeStyles(sharedStyles);

const columns = [
  {
    name: "name",
    renderer: (name) => <NavLink path={`/taskDef/${name}`}>{name}</NavLink>,
  },
  { name: "description", grow: 2 },
  { name: "createTime", type: "date" },
  { name: "ownerEmail" },
  { name: "inputKeys", type: "json", sortable: false },
  { name: "outputKeys", type: "json", sortable: false },
  { name: "timeoutPolicy", grow: 0.5 },
  { name: "timeoutSeconds", grow: 0.5 },
  { name: "retryCount", grow: 0.5 },
  { name: "retryLogic" },
  { name: "retryDelaySeconds", grow: 0.5 },
  { name: "responseTimeoutSeconds", grow: 0.5 },
  { name: "inputTemplate", type: "json", sortable: false },
  { name: "rateLimitPerFrequency", grow: 0.5 },
  { name: "rateLimitFrequencyInSeconds", grow: 0.5 },
  {
    name: "name",
    label: "Executions",
    id: "executions_link",
    grow: 0.5,
    renderer: (name) => (
      <NavLink path={`/search/by-tasks?tasks=${name}`} newTab>
        Query
      </NavLink>
    ),
    sortable: false,
    searchable: false,
  },
];

export default function TaskDefinitions() {
  const classes = useStyles();

  const { data: tasks, isFetching } = useFetch("/metadata/taskdefs");

  return (
    <div className={classes.wrapper}>
      <Helmet>
        <title>Conductor UI - Task Definitions</title>
      </Helmet>

      <Header tabIndex={1} loading={isFetching} />

      <div className={classes.tabContent}>
        {tasks && (
          <DataTable
            title={`${tasks.length} results`}
            localStorageKey="tasksTable"
            defaultShowColumns={[
              "name",
              "description",
              "ownerEmail",
              "timeoutPolicy",
              "retryCount",
              "executions_link",
            ]}
            keyField="name"
            default
            data={tasks}
            columns={columns}
          />
        )}
      </div>
    </div>
  );
}
