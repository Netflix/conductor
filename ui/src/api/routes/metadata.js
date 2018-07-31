import MetadataService from './services/metadata.service';

class MetadataRoutes {
  constructor() {
    this.metadataService = new MetadataService();
  }

  init(app) {
    app.get('/metadata/workflow/:name/:version', async (req, res, next) => {
      try {
        const {
          params: { name, version },
          token
        } = req;

        const result = await this.metadataService.get(name, version, token);
        res.status(200).send({ result });
      } catch (err) {
        next(err);
      }
    });

    app.get('/metadata/workflow', async (req, res, next) => {
      try {
        const { token } = req;

        const result = await this.metadataService.list(token);
        res.status(200).send({ result });
      } catch (err) {
        next(err);
      }
    });

    app.get('/metadata/taskdef', async (req, res, next) => {
      try {
        const { token } = req;

        const result = await this.metadataService.taskDef(token);
        res.status(200).send({ result });
      } catch (err) {
        next(err);
      }
    });
  }
}

module.exports = MetadataRoutes;
