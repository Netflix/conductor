import BaseService from './base.service';

class MetadataService extends BaseService {
  async list(token) {
    const result = await this.get('metadata/workflow', token);

    return { result };
  }

  async getByName(name, version, token) {
    const result = await this.get(`metadata/workflow/${name}?version=${version}`, token);

    return { result };
  }

  async taskDef(token) {
    const result = await this.get('metadata/taskdefs', token);

    return { result };
  }
}

module.exports = MetadataService;
