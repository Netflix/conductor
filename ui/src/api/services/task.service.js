/* eslint-disable no-restricted-globals,consistent-return */
import BaseService from './base.service';

class TasksService extends BaseService {
  constructor() {
    super();

    const { TASKS_API_BASE_ROUTE = 'tasks' } = process.env;

    this.baseTasksRoute = TASKS_API_BASE_ROUTE;
  }

  async queueList(token) {
    const sizes = await this.get(`${this.baseTasksRoute}/queue/all`, token);
    const polldata = await this.get(`${this.baseTasksRoute}/queue/polldata/all`, token);
    polldata.forEach(pd => {
      let qname = pd.queueName;

      if (pd.domain != null) {
        qname = `${pd.domain}:${qname}`;
      }
      pd.qsize = sizes[qname];
    });

    return { polldata };
  }

  async taskLog(taskId, token) {
    const logs = await this.get(`${this.baseTasksRoute}/${taskId}/log`, token);
    return { logs };
  }
}

module.exports = TasksService;
