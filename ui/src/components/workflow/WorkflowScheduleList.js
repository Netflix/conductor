import React, { Component } from 'react';
import { Button, ButtonGroup } from 'react-bootstrap';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';
import { connect } from 'react-redux';
import request from 'superagent';
import {Link} from "react-router";
import {getCronData} from "../../actions/WorkflowActions";

const WorkflowScheduleList = React.createClass({

  getInitialState() {
    return {
        cronJobs: this.props.cronData.jobs,
        cronHistory: this.props.cronData.history
    }
  },

  componentWillMount(){
    this.props.dispatch(getCronData());
  },

  componentWillReceiveProps(nextProps){
      this.setState({
          cronJobs: nextProps.cronData.jobs,
          cronHistory: nextProps.cronData.history
      });
  },

  stopCronJob(cronjobId) {
    request
      .post('/api/wfe/cronjobs/stop/' + cronjobId)
      .end(function (err, res) {
        if (err) {
          console.log(res)
        } else {
          window.location.reload();
        }
      });
  },

  deleteCronJob(cronjobId) {
    request
      .post('/api/wfe/cronjobs/stop/' + cronjobId)
      .end(function (err, res) {
        if (err) {
          console.log(res)
        } else {
          request
            .delete('/api/wfe/cronjobs/delete/' + cronjobId)
            .end(function (err, res) {
              if (err) {
                console.log(res)
              } else {
                console.log(res);
                window.location.reload();
              }
            });
        }
      });
  },

  startCronJob(cronjobId) {
    request
      .post('/api/wfe/cronjobs/start/' + cronjobId)
      .end(function (err, res) {
        if (err) {
          console.log(res)
        } else {
          console.log(res);
          window.location.reload();
        }
      });
  },

  render() {
    let { cronHistory } = this.state;
    let cronJobs = this.state.cronJobs || [];

    function runningMaker(cell, row){
      if(cell.running === true){
        return 'Running';
      }
      return 'Stopped';
    }

    function executionMaker(cell, row){
        if(cronHistory.length > 0 && cell.lastExecution != null) {
            for(let i = cronHistory.length-1; i >= 0; i-- ){
                if(cronHistory[i].id === row.id){
                    return (<Link to={`/workflow/id/${cronHistory[i].wfid}`}>{cell.lastExecution}</Link>);
                }
            }
        }
    }

    function miniDetails(cell, row){
      return (<ButtonGroup>
        <Button bsSize="xsmall"
                bsStyle={ row.context.running ? "default" : "success" }
                onClick={ row.context.running ?
                () => {this.stopCronJob(row.id)}
                :
                () => {this.startCronJob(row.id)} }>
                { row.context.running ? "Stop" : "Start" }</Button>
        <Button bsStyle="danger"
                bsSize="xsmall"
                onClick={ () => {this.deleteCronJob(row.id)} }>Delete</Button>
        </ButtonGroup>
      );
     }

    return (
      <div className="ui-content">
        <h1>Scheduled Workflows</h1>
        <BootstrapTable ref="table" data={cronJobs} striped={true} hover={true} search={true} exportCSV={false} pagination={false}>
          <TableHeaderColumn dataField="name" width="350">Workflow</TableHeaderColumn>
          <TableHeaderColumn dataField="id" width="120" isKey={true}>Workflow ID</TableHeaderColumn>
          <TableHeaderColumn dataField="cronTime" width="200">Cron Time</TableHeaderColumn>
          <TableHeaderColumn dataField="desc">Description</TableHeaderColumn>
          <TableHeaderColumn dataField="context" width="200" dataFormat={executionMaker}>Last Execution</TableHeaderColumn>
          <TableHeaderColumn dataField="context" width="100" dataFormat={runningMaker}>Status</TableHeaderColumn>
          <TableHeaderColumn dataField="" width="100" dataFormat={miniDetails.bind(this)}>Actions</TableHeaderColumn>
        </BootstrapTable>
      </div>
    );
  }
});
export default connect(state => state.workflow)(WorkflowScheduleList);
