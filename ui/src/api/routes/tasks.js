import TaskService from '../services/task.service';

class WorkflowRoutes {
  constructor() {
    this.taskService = new TaskService();
  }

  init(app) {
    app.get('/api/wfe/queue/data', async (req, res, next) => {
      try {
        const { token } = req;
        const polldata = await this.taskService.queueList(token);
        res.status(200).send(polldata);
      } catch (err) {
        next(err);
      }
    });

    app.get('/task/log/:taskId', async (req, res, next) => {
      try {
        const {
          params: { taskId },
          token
        } = req;

        const logs = await this.taskService.taskLog(taskId, token);
        res.status(200).send(logs);
      } catch (err) {
        next(err);
      }
    });
  }
}

module.exports = WorkflowRoutes;
