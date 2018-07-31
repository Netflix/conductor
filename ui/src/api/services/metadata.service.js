import BaseService from './base.service';

class MetadataService extends BaseService {
  async list(token) {
    const result = await this.get('workflow', token);

    return result;
  }

  async get(name, version, token) {
    const result = await this.get(`workflow/${name}?version=${version}`, token);

    return result;
  }

  async taskDef(token) {
    const result = await this.get('workflow/taskdefs', token);

    return { result };
  }
}

module.exports = MetadataService;
