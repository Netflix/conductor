import WorflowService from './services/workflow.service';

class WorkflowRoutes {
  constructor() {
    this.worflowService = new WorflowService();
  }

  init(app) {
    app.get('/api/wfe/', async (req, res, next) => {
      try {
        const result = await this.worflowService.search(req);
        return res.status(200).json(result);
      } catch (e) {
        return next(e);
      }
    });

    app.get('/api/wfe/search-by-task/:taskId', async (req, res, next) => {
      try {
        const result = await this.worflowService.searchByTask(req);
        return res.status(200).json(result);
      } catch (e) {
        return next(e);
      }
    });

    app.get('/api/wfe/id/:workflowId', async (req, res, next) => {
      try {
        const { workflowId } = req.params;
        const result = await this.worflowService.getByWorkflowId(req, workflowId);
        res.status(200).send(result);
      } catch (err) {
        next(err);
      }
    });

    app.delete('/api/wfe/terminate/:workflowId', async (req, res, next) => {
      try {
        const {
          params: { workflowId },
          token
        } = req.params;
        await this.worflowService.terminate(workflowId, token);

        res.status(200).send({ result: workflowId });
      } catch (err) {
        next(err);
      }
    });

    app.post('/api/wfe/restart/:workflowId', async (req, res, next) => {
      try {
        const {
          params: { workflowId },
          token
        } = req;

        await this.worflowService.restart(workflowId, token);

        res.status(200).send({ result: workflowId });
      } catch (err) {
        next(err);
      }
    });

    app.post('/api/wfe/retry/:workflowId', async (req, res, next) => {
      try {
        const {
          params: { workflowId },
          token
        } = req;

        await this.worflowService.retry(workflowId, token);
        res.status(200).send({ result: workflowId });
      } catch (err) {
        next(err);
      }
    });

    app.post('/api/wfe/pause/:workflowId', async (req, res, next) => {
      try {
        const {
          params: { workflowId },
          token
        } = req;

        await this.worflowService.pause(workflowId, token);
        res.status(200).send({ result: workflowId });
      } catch (err) {
        next(err);
      }
    });

    app.post('/api/wfe/resume/:workflowId', async (req, res, next) => {
      try {
        const {
          params: { workflowId },
          token
        } = req;

        await this.worflowService.resume(workflowId, token);
        res.status(200).send({ result: workflowId });
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

        const logs = await this.worflowService.queueData(taskId, token);
        res.status(200).send(logs);
      } catch (err) {
        next(err);
      }
    });

    app.get('/queue/data', async (req, res, next) => {
      try {
        const { token } = req;
        const polldata = await this.worflowService.queueData(token);
        res.status(200).send(polldata);
      } catch (err) {
        next(err);
      }
    });
  }
}

module.exports = WorkflowRoutes;
