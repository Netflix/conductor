import {Router} from 'express';
import http from '../core/HttpClient';
import lookup from '../core/ApiLookup';

const router = new Router();

router.get('/', async (req, res, next) => {
  try {
    const baseURL = await lookup.lookup();
    const baseURL2 = baseURL + 'event/';

    const result = await http.get(baseURL2);
    res.status(200).send(result);

  } catch (err) {
    next(err);
  }
});


router.get('/executions', async (req, res, next) => {
  try {
    const baseURL = await lookup.lookup();
    const baseURL2 = baseURL + 'event/';

    let event = req.params.event;
    const result = await http.get(baseURL2 + '/' + req.params.event + '?activeOnly=false');
    res.status(200).send(result);

  } catch (err) {
    next(err);
  }
});


module.exports = router;
