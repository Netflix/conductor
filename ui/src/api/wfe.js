import {Router} from 'express';
import http from '../core/HttpClient';
import moment from 'moment';
import lookup from '../core/ApiLookup';

const router = new Router();

function getToken(req) {
  const authorization = req.get('Authorization');
  return authorization ? authorization.substr(7) : null; // 'Bearer ...'
}

router.get('/', async (req, res, next) => {

  try {
    const baseURL = await lookup.lookup();
    const baseURL2 = baseURL + 'workflow/';

    let freeText = [];
    if (req.query.freeText != '') {
      freeText.push(req.query.freeText);
    } else {
      freeText.push('*');
    }

    let h = '-1';
    if (req.query.h != 'undefined' && req.query.h != '') {
      h = req.query.h;
    }

    var frmdate = req.query.frmdate;
    var todate = req.query.todate;
    let range = req.query.range;
    let from = null;
    let end = null;
    if (frmdate != 'undefined' && todate != 'undefined' && frmdate != '' && todate != '') {

      from = moment(frmdate);
      end = moment(todate);
    } else {
      if (h != '-1') {
        from = moment().subtract(h, 'hours');
        end = moment();
      } else if (range === 'All data') {
        // do nothing
      } else if (range === 'This year') {
        from = moment().startOf('year');
        end = moment().endOf('year');
      } else if (range === 'Last quarter') {
        from = moment().subtract(1, 'quarter').startOf('quarter');
        end = moment().subtract(1, 'quarter').endOf('quarter');
      } else if (range === 'This quarter') {
        from = moment().startOf('quarter');
        end = moment().endOf('quarter');
      } else if (range === 'Last month') {
        from = moment().subtract(1, 'month').startOf('month');
        end = moment().subtract(1, 'month').endOf('month');
      } else if (range === 'This month') {
        from = moment().startOf('month');
        end = moment().endOf('month');
      } else if (range === 'Yesterday') {
        from = moment().subtract(1, 'days').startOf('day');
        end = moment().subtract(1, 'days').endOf('day');
      } else if (range === 'Last 30 minutes') {
        from = moment().subtract(30, 'minutes').startOf('minute');
        end = moment();
      } else if (range === 'Last 5 minutes') {
        from = moment().subtract(5, 'minutes').startOf('minute');
        end = moment();
      } else { // Today is default
        from = moment().startOf('day');
        end = moment().endOf('day');
      }
    }

    if (from != null && end != null) {
      freeText.push('startTime:[' + from.toISOString() + ' TO ' + end.toISOString() + ']');
    }

    let start = 0;
    if (!isNaN(req.query.start)) {
      start = req.query.start;
    }

    let query = req.query.q;

    const url = baseURL2 + 'search?size=100&sort=startTime:DESC&freeText=' + freeText.join(' AND ') + '&start=' + start + '&query=' + query;
    const result = await http.get(url);
    const hits = result.results;
    res.status(200).send({result: {hits: hits, totalHits: result.totalHits}});
  } catch (err) {
    next(err);
  }
});

router.get('/id/:workflowId', async (req, res, next) => {
  try {
    const baseURL = await lookup.lookup();
    const baseURL2 = baseURL + 'workflow/';
    const baseURLMeta = baseURL + 'metadata/';
    const baseURLTask = baseURL + 'tasks/';

    let s = new Date().getTime();
    const result = await http.get(baseURL2 + req.params.workflowId + '?includeTasks=true');
    const meta = await http.get(baseURLMeta + 'workflow/' + result.workflowType + '?version=' + result.version);
    const subs = [];
    const subworkflows = {};

    // Work around in case server did not return tasks
    if (result.tasks === undefined) {
        result.tasks = [];
    }

    result.tasks.forEach(task => {
      if(task.taskType == 'SUB_WORKFLOW'){
        let subWorkflowId = task.outputData && task.outputData.subWorkflowId;
        if(subWorkflowId == null) {
          subWorkflowId = task.inputData.subWorkflowId;
        }
        if(subWorkflowId != null) {
          subs.push({name: task.inputData.subWorkflowName, version: task.inputData.subWorkflowVersion, referenceTaskName: task.referenceTaskName, subWorkflowId: subWorkflowId});
        }
      }
    });
    for(let t = 0; t < result.tasks.length; t++) {
      result.tasks[t].logs = [];
      // let task = result.tasks[t];
      // let logs = await http.get(baseURLTask + task.taskId + '/log');
      // logs = logs || [];
      // let logs2 = [];
      // logs.forEach(log => {
      //   const dtstr = moment(log.createdTime).format('MM/DD/YY, HH:mm:ss:SSS');
      //   logs2.push(dtstr + ' : ' + log.log);
      // });
      // task.logs = logs2;
    }
    let submeta = {};
    for(let i = 0; i < subs.length; i++){
      let submeta = await http.get(baseURLMeta + 'workflow/' + subs[i].name + '?version=' + subs[i].version);
      let subes = await http.get(baseURL2 + subs[i].subWorkflowId + '?includeTasks=true');
      let prefix = subs[i].referenceTaskName;
      subworkflows[prefix] = {meta: submeta, wfe: subes};
    }
    let e = new Date().getTime();
    let time = e-s;

    res.status(200).send({result, meta, subworkflows:subworkflows});
  } catch (err) {
    next(err);
  }
});

router.delete('/terminate/:workflowId', async (req, res, next) => {
  try {
    const token = getToken(req);
    const baseURL = await lookup.lookup();
    const baseURL2 = baseURL + 'workflow/';

    const result = await http.delete(baseURL2 + req.params.workflowId, null, token);
    res.status(200).send({result: req.params.workflowId});
  } catch (err) {
    next(err);
  }
});

router.post('/cancel/:workflowId', async (req, res, next) => {
  try {
    const token = getToken(req);
    const baseURL = await lookup.lookup();
    const baseURL2 = baseURL + 'workflow/';

    const result = await http.postPlain(baseURL2 + req.params.workflowId + '/cancel', null, token);
    res.status(200).send({result: req.params.workflowId});
  } catch (err) {
    console.log("err", err);
    next(err);
  }
});

router.post('/restart/:workflowId', async (req, res, next) => {
  try {
    const token = getToken(req);
    const baseURL = await lookup.lookup();
    const baseURL2 = baseURL + 'workflow/';

    const result = await http.post(baseURL2 + req.params.workflowId + '/restart', null, token);
    res.status(200).send({result: req.params.workflowId});
  } catch (err) {
    next(err);
  }
});

router.post('/retry/:workflowId', async (req, res, next) => {
  try {
    const token = getToken(req);
    const baseURL = await lookup.lookup();
    const baseURL2 = baseURL + 'workflow/';

    const result = await http.post(baseURL2 + req.params.workflowId + '/retry', null, token);
    res.status(200).send({result: req.params.workflowId});
  } catch (err) {
    next(err);
  }
});

router.post('/pause/:workflowId', async (req, res, next) => {
  try {
    const token = getToken(req);
    const baseURL = await lookup.lookup();
    const baseURL2 = baseURL + 'workflow/';

    const result = await http.put(baseURL2 + req.params.workflowId + '/pause', null, token);
    res.status(200).send({result: req.params.workflowId});
  } catch (err) {
    next(err);
  }
});

router.post('/resume/:workflowId', async (req, res, next) => {
  try {
    const token = getToken(req);
    const baseURL = await lookup.lookup();
    const baseURL2 = baseURL + 'workflow/';

    const result = await http.put(baseURL2 + req.params.workflowId + '/resume', null, token);
    res.status(200).send({result: req.params.workflowId});
  } catch (err) {
    next(err);
  }
});

//metadata
router.get('/metadata/workflow/:name/:version', async (req, res, next) => {
  try {
    const baseURL = await lookup.lookup();
    const baseURLMeta = baseURL + 'metadata/';

    const result = await http.get(baseURLMeta + 'workflow/' + req.params.name + '?version=' + req.params.version);
    res.status(200).send({result});
  } catch (err) {
    next(err);
  }
});

router.get('/metadata/workflow', async (req, res, next) => {
  try {
    const baseURL = await lookup.lookup();
    const baseURLMeta = baseURL + 'metadata/';

    const result = await http.get(baseURLMeta + 'workflow');
    res.status(200).send({result});
  } catch (err) {
    next(err);
  }
});

router.get('/metadata/taskdef', async (req, res, next) => {
  try {
    const baseURL = await lookup.lookup();
    const baseURLMeta = baseURL + 'metadata/';

    const result = await http.get(baseURLMeta + 'taskdefs');
    res.status(200).send({result});
  } catch (err) {
    next(err);
  }
});

router.get('/task/log/:taskId', async (req, res, next) => {
  try {
    const baseURL = await lookup.lookup();
    const baseURLTask = baseURL + 'tasks/';

    const logs = await http.get(baseURLTask + req.params.taskId + '/log');
    res.status(200).send({logs});
  } catch (err) {
    next(err);
  }
});

router.get('/queue/data', async (req, res, next) => {
  try {
    const baseURL = await lookup.lookup();
    const baseURLTask = baseURL + 'tasks/';

    const sizes = await http.get(baseURLTask + 'queue/all');
    const polldata = await http.get(baseURLTask + 'queue/polldata/all');
    polldata.forEach(pd => {
      var qname = pd.queueName;

      if (pd.domain != null) {
        qname = pd.domain + ":" + qname;
      }
      pd.qsize = sizes[qname];
    });
    res.status(200).send({polldata});
  } catch (err) {
    next(err);
  }
});

module.exports = router;