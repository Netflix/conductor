/* eslint-disable consistent-return */
import axios from 'axios';

const searchWorkflowsByTaskId = (dispatch, search, hours, start) => {
  return axios.get(`/api/wfe/search-by-task/${search}?h=${hours}&start=${start}`).then(({ data }) => {
    dispatch({
      type: 'RECEIVED_WORKFLOWS',
      data
    });
  });
};

export const searchWorkflows = (query, search, hours, fullstr, start) => {
  return dispatch => {
    dispatch({
      type: 'GET_WORKFLOWS',
      search
    });


    return axios
      .get(
        `/api/wfe?q=${query}&h=${hours}&freeText=${
          fullstr && search != null && search.length > 0 ? `"${search}"` : search
        }&start=${start}`
      )
      .then(({ data }) => {
        if (data && data.result && data.result.totalHits > 0) {
          dispatch({
            type: 'RECEIVED_WORKFLOWS',
            data
          });
        } else if (search !== '') {
          return searchWorkflowsByTaskId(dispatch, search, hours, start);
        }
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const getWorkflowDetails = workflowId => {
  return dispatch => {
    dispatch({
      type: 'GET_WORKFLOW_DETAILS',
      workflowId
    });

    return axios
      .get(`/api/wfe/id/${workflowId}`)
      .then(({ data }) => {
        dispatch({
          type: 'RECEIVED_WORKFLOW_DETAILS',
          data
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const terminateWorkflow = workflowId => {
  return dispatch => {
    dispatch({
      type: 'REQUESTED_TERMINATE_WORKFLOW',
      workflowId
    });

    return axios
      .delete(`/api/wfe/terminate/${workflowId}`)
      .then(() => {
        dispatch({
          type: 'RECEIVED_TERMINATE_WORKFLOW',
          workflowId
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const restartWorfklow = workflowId => {
  return dispatch => {
    dispatch({
      type: 'REQUESTED_RESTART_WORKFLOW',
      workflowId
    });

    return axios
      .post(`/api/wfe/restart/${workflowId}`)
      .then(() => {
        dispatch({
          type: 'RECEIVED_RESTART_WORKFLOW',
          workflowId
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const retryWorfklow = workflowId => {
  return dispatch => {
    dispatch({
      type: 'REQUESTED_RETRY_WORKFLOW',
      workflowId
    });

    return axios
      .post(`/api/wfe/retry/${workflowId}`)
      .then(() => {
        dispatch({
          type: 'RECEIVED_RETRY_WORKFLOW',
          workflowId
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const pauseWorfklow = workflowId => {
  return dispatch => {
    dispatch({
      type: 'REQUESTED_PAUSE_WORKFLOW',
      workflowId
    });

    return axios
      .post(`/api/wfe/pause/${workflowId}`)
      .then(() => {
        dispatch({
          type: 'RECEIVED_PAUSE_WORKFLOW',
          workflowId
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const resumeWorfklow = workflowId => {
  return dispatch => {
    dispatch({
      type: 'REQUESTED_RESUME_WORKFLOW',
      workflowId
    });

    return axios
      .post(`/api/wfe/resume/${workflowId}`)
      .then(() => {
        dispatch({
          type: 'RECEIVED_RESUME_WORKFLOW',
          workflowId
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

// metadata
export const getWorkflowDefs = () => {
  return dispatch => {
    dispatch({
      type: 'LIST_WORKFLOWS'
    });

    return axios
      .get('/api/wfe/metadata/workflow')
      .then(({ data }) => {
        dispatch({
          type: 'RECEIVED_LIST_WORKFLOWS',
          workflows: data
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const getWorkflowMetaDetails = (name, version) => {
  return dispatch => {
    dispatch({
      type: 'GET_WORKFLOW_DEF',
      name,
      version
    });

    return axios
      .get(`/api/wfe/metadata/workflow/${name}/${version}`)
      .then(({ data }) => {
        dispatch({
          type: 'RECEIVED_WORKFLOW_DEF',
          name,
          version,
          workflowMeta: data
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const getTaskDefs = () => {
  return dispatch => {
    dispatch({
      type: 'GET_TASK_DEFS'
    });

    return axios
      .get('/api/wfe/metadata/taskdef')
      .then(({ data }) => {
        dispatch({
          type: 'RECEIVED_TASK_DEFS',
          taskDefs: data
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const getQueueData = () => {
  return dispatch => {
    dispatch({
      type: 'GET_POLL_DATA'
    });

    return axios
      .get('/api/wfe/queue/data')
      .then(({ data: queueData }) => {
        dispatch({
          type: 'RECEIVED_POLL_DATA',
          queueData
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const updateWorkflow = workflow => {
  return dispatch => {
    dispatch({
      type: 'REQUESTED_UPDATE_WORKFLOW_DEF',
      workflow
    });

    return axios
      .put('/api/wfe/metadata/', workflow)
      .then(() => {
        dispatch({
          type: 'RECEIVED_UPDATE_WORKFLOW_DEF'
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const getEventHandlers = () => {
  return dispatch => {
    dispatch({
      type: 'LIST_EVENT_HANDLERS'
    });

    return axios
      .get('/api/events')
      .then(({ data: events }) => {
        dispatch({
          type: 'RECEIVED_LIST_EVENT_HANDLERS',
          events
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const getEvents = () => {
  return dispatch => {
    dispatch({
      type: 'LIST_EVENT'
    });

    return axios
      .get('/api/events/executions')
      .then(({ data }) => {
        dispatch({
          type: 'RECEIVED_LIST_EVENT',
          events: data
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};

export const getTaskLogs = taskId => {
  return dispatch => {
    dispatch({
      type: 'GET_TASK_LOGS'
    });

    return axios
      .get(`/api/wfe/task/log${taskId}`)
      .then(({ data: logs }) => {
        dispatch({
          type: 'RECEIVED_GET_TASK_LOGS',
          logs
        });
      })
      .catch(e => {
        dispatch({
          type: 'REQUEST_ERROR',
          e
        });
      });
  };
};
