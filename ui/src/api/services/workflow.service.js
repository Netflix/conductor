/* eslint-disable no-restricted-globals,consistent-return */
import identity from 'lodash/identity';
import moment from 'moment';
import filter from 'lodash/fp/filter';
import forEach from 'lodash/fp/forEach';
import map from 'lodash/fp/map';
import transform from 'lodash/transform';
import BaseService from './base.service';

const LOG_DATE_FORMAT = 'MM/DD/YY, HH:mm:ss:SSS';

class WorkflowService extends BaseService {
  constructor() {
    super();

    const {
      WORKFLOW_API_BASE_ROUTE = 'workflow',
      META_API_BASE_ROUTE = 'metadata',
      TASKS_API_BASE_ROUTE = 'tasks'
    } = process.env;

    this.baseWorkflowRoute = WORKFLOW_API_BASE_ROUTE;
    this.baseMetadataRoute = META_API_BASE_ROUTE;
    this.baseTasksRoute = TASKS_API_BASE_ROUTE;
  }

  async search(reqFreeText, reqStart, reqH, q, token) {
    const freeText = [];
    if (reqFreeText !== '') {
      freeText.push(reqFreeText);
    } else {
      freeText.push('*');
    }

    let h = '-1';
    if (reqH && reqH !== 'undefined' && reqH !== '') {
      h = reqH;
    }

    if (h !== '-1') {
      freeText.push(`startTime:[now-${h}h TO now]`);
    }

    let start = 0;
    if (!isNaN(reqStart)) {
      start = reqStart;
    }

    const url = `${this.baseWorkflowRoute}/search?size=100&sort=startTime:DESC&freeText=${freeText.join(
      ' AND '
    )}&start=${start}&query=${q}`;

    const { results: hits, totalHits } = await this.get(url, token);

    return { result: { hits, totalHits } };
  }

  async searchByTask(taskId, reqFreeText, reqStart, reqH, token) {
    const freeText = [];
    if (reqFreeText !== '') {
      freeText.push(taskId);
    } else {
      freeText.push('*');
    }

    let h = '-1';
    if (reqH !== undefined && reqH !== 'undefined' && reqH !== '') {
      h = reqH;
    }
    if (h !== '-1') {
      freeText.push(`startTime:[now-${h}h TO now]`);
    }
    let start = 0;
    if (!isNaN(reqStart)) {
      start = reqStart;
    }

    const url = `${this.baseWorkflowRoute}/search-by-tasks?size=100&sort=startTime:DESC&freeText=${freeText.join(
      ' AND '
    )}&start=${start}`;
    const { hits, totalHits } = await this.get(url, token);

    return { result: { hits, totalHits } };
  }

  async getByWorkflowId(workflowId, token) {
    const data = await this.get(`${this.baseWorkflowRoute}/${workflowId}?includeTasks=true`, token);
    const meta = await this.get(
      `${this.baseMetadataRoute}/workflow/${data.workflowType}?version=${data.version}`,
      token
    );

    const subs = filter(identity)(
      map(t1 => {
        if (t1.taskType === 'SUB_WORKFLOW') {
          const subWorkflowId = t1.inputData && t1.inputData.subWorkflowId;

          if (subWorkflowId != null) {
            return {
              name: t1.inputData.subWorkflowName,
              version: t1.inputData.subWorkflowVersion,
              referenceTaskName: t1.referenceTaskName,
              subWorkflowId
            };
          }
        }
      })(data.tasks || [])
    );

    (data.tasks || []).forEach(task => {
      if (task.taskType === 'SUB_WORKFLOW') {
        const subWorkflowId = task.inputData && task.inputData.subWorkflowId;

        if (subWorkflowId != null) {
          subs.push({
            name: task.inputData.subWorkflowName,
            version: task.inputData.subWorkflowVersion,
            referenceTaskName: task.referenceTaskName,
            subWorkflowId
          });
        }
      }
    });

    const logs = map(task => Promise.all([task, this.get(`${this.baseTasksRoute}/${task.taskId}/log`)]))(data.tasks);

    await Promise.all(logs).then(result => {
      forEach(([task, logs]) => {
        if (logs) {
          task.logs = map(({ createdTime, log }) => `${moment(createdTime).format(LOG_DATE_FORMAT)} : ${log}`)(logs);
        }
      })(result);
    });

    const promises = map(({ name, version, subWorkflowId, referenceTaskName }) =>
      Promise.all([
        referenceTaskName,
        this.get(`${this.baseMetadataRoute}/workflow/${name}?version=${version}`, token),
        this.get(`${this.baseWorkflowRoute}/${subWorkflowId}?includeTasks=true`, token)
      ])
    )(subs);

    const subworkflows = await Promise.all(promises).then(result =>
      transform(
        result,
        (result, [key, meta, wfe]) => {
          result[key] = { meta, wfe };
        },
        {}
      )
    );

    return { result: data, meta, subworkflows };
  }

  async terminate(workflowId, token) {
    await this.delete(`${this.baseWorkflowRoute}/${workflowId}`, token);
  }

  async restart(workflowId, token) {
    await this.post(`${this.baseWorkflowRoute}/${workflowId}/restart`, token);
  }

  async retry(workflowId, token) {
    await this.post(`${this.baseWorkflowRoute}/${workflowId}/retry`, token);
  }

  async pause(workflowId, token) {
    await this.put(`${this.baseWorkflowRoute}/${workflowId}/pause`, token);
  }

  async resume(workflowId, token) {
    await this.put(`${this.baseWorkflowRoute}/${workflowId}/resume`, token);
  }
}

module.exports = WorkflowService;
