import WorflowService from '../services/workflow.service';

class WorkflowRoutes {
  constructor() {
    this.worflowService = new WorflowService();
  }

  init(app) {
    app.get('/api/wfe/', async (req, res, next) => {
      try {
        const {
          query: { freeText: reqFreeText = '', start: reqStart = '', h: reqH, q = '' },
          token
        } = req;
        const result = await this.worflowService.search(reqFreeText, reqStart, reqH, q, token);
        return res.status(200).json(result);
      } catch (e) {
        return next(e);
      }
    });

    app.get('/api/wfe/search-by-task/:taskId', async (req, res, next) => {
      try {
        const {
          query: { freeText: reqFreeText = '', start: reqStart = '', h: reqH },
          params: { taskId },
          token
        } = req;
        const result = await this.worflowService.searchByTask(taskId, reqFreeText, reqStart, reqH, token);
        return res.status(200).json(result);
      } catch (e) {
        return next(e);
      }
    });

    app.get('/api/wfe/id/:workflowId', async (req, res, next) => {
      try {
        const {
          params: { workflowId },
          token
        } = req;
        const result = await this.worflowService.getByWorkflowId(workflowId, token);
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
        } = req;
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
  }
}

module.exports = WorkflowRoutes;
