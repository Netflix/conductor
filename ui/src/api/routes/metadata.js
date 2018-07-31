import MetadataService from '../services/metadata.service';

class MetadataRoutes {
  constructor() {
    this.metadataService = new MetadataService();
  }

  init(app) {
    app.get('/api/wfe/metadata/workflow', async (req, res, next) => {
      try {
        const { token } = req;

        const result = await this.metadataService.list(token);
        res.status(200).json(result);
      } catch (err) {
        next(err);
      }
    });

    app.get('/api/wfe/metadata/workflow/:name/:version', async (req, res, next) => {
      try {
        const {
          params: { name, version },
          token
        } = req;

        const result = await this.metadataService.getByName(name, version, token);
        res.status(200).json(result);
      } catch (err) {
        next(err);
      }
    });

    app.get('/api/wfe/metadata/taskdef', async (req, res, next) => {
      try {
        const { token } = req;

        const result = await this.metadataService.taskDef(token);
        res.status(200).json(result);
      } catch (err) {
        next(err);
      }
    });
  }
}

module.exports = MetadataRoutes;
