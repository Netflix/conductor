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
  async search(req) {
    const { freeText: reqFreeText = '', start: reqStart = '', h: reqH, q = '' } = req;

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

    const url = `workflow/search?size=100&sort=startTime:DESC&freeText=${freeText.join(
      ' AND '
    )}&start=${start}&query=${q}`;

    const { results: hits, totalHits } = await this.get(url, req.token);

    return { result: { hits, totalHits } };
  }

  async searchByTask(req) {
    const { freeText: reqFreeText = '', start: reqStart = '', h: reqH } = req;

    const freeText = [];
    if (reqFreeText !== '') {
      freeText.push(req.params.taskId);
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

    const url = `search-by-tasks?size=100&sort=startTime:DESC&freeText=${freeText.join(' AND ')}&start=${start}`;
    const { hits, totalHits } = await this.get(url, req.token);

    return { result: { hits, totalHits } };
  }

  async getByWorkflowId(req, workflowId) {
    const data = await this.get(`workflow/${workflowId}?includeTasks=true`, req.token);
    const meta = await this.get(`metadata/workflow/${data.workflowType}?version=${data.version}`, req.token);

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

    const logs = map(task => Promise.all([task, this.get(`tasks/${task.taskId}/log`)]))(data.tasks);

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
        this.get(`metadata/workflow/${name}?version=${version}`),
        this.get(`workflow/${subWorkflowId}?includeTasks=true`)
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
    console.log(workflowId, token);
    await this.delete(`workflow/${workflowId}`, token);
  }

  async restart(workflowId, token) {
    await this.post(`workflow/${workflowId}/restart`, token);
  }

  async retry(workflowId, token) {
    await this.post(`workflow/${workflowId}/retry`, token);
  }

  async pause(workflowId, token) {
    await this.put(`workflow/${workflowId}/pause`, token);
  }

  async resume(workflowId, token) {
    await this.put(`workflow/${workflowId}/resume`, token);
  }

  async taskLog(taskId, token) {
    const logs = await this.get(`tasks/${taskId}/log`, token);
    return { logs };
  }

  async queueData(token) {
    const sizes = await this.get('tasks/queue/all', token);
    const polldata = await this.get('tasks/queue/polldata/all', token);
    polldata.forEach(pd => {
      let qname = pd.queueName;

      if (pd.domain != null) {
        qname = `${pd.domain}:${qname}`;
      }
      pd.qsize = sizes[qname];
    });

    return { polldata };
  }
}

module.exports = WorkflowService;
