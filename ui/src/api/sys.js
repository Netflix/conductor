import {Router} from 'express';
import http from '../core/HttpClient';
import lookup from '../core/ApiLookup';

const router = new Router();

router.get('/', async (req, res, next) => {

  const server = await lookup.lookup();

  try {
    const result = {
      server: server,
      env: process.env
    };
    const config = await http.get(server + 'admin/config');
    result.version = config.version;
    result.buildDate = config.buildDate;
    res.status(200).send({sys: result});
  } catch (err) {
    next(err);
  }
});
module.exports = router;
