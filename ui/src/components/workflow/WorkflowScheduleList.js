import React, { Component } from 'react';
import { Button, ButtonGroup } from 'react-bootstrap';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';
import { connect } from 'react-redux';
import request from 'superagent';

const WorkflowScheduleList = React.createClass({

  getInitialState() {
    return {
      cronJobs: []
    }
  },

  componentWillMount(){
    this.getCronJobs();
  },

  componentWillReceiveProps(){
    this.getCronJobs();
  },

  getCronJobs() {
    let self = this;
    request
      .get('/api/wfe/cronjobs')
      .end(function (err, res) {
        if (err) {
          console.log(res)
        } else {
          console.log(res)
          self.setState({
            cronJobs: JSON.parse(res.text)
          })
        }
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
    var { cronJobs } = this.state;  

    function runningMaker(cell, row){
      if(cell.running == true){
        return 'Running';
      }
      return 'Stopped';
    };

    function executionMaker(cell, row){
      if(cell.lastExecution == null){
        return '';
      }
      return cell.lastExecution;
    };

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
     };

    return (
      <div className="ui-content">
        <h1>Scheduled Workflows</h1>
        <BootstrapTable ref="table" data={cronJobs} striped={true} hover={true} search={true} exportCSV={false} pagination={false}>
          <TableHeaderColumn dataField="name">Name</TableHeaderColumn>
          <TableHeaderColumn dataField="id" isKey={true}>ID</TableHeaderColumn>
          <TableHeaderColumn dataField="cronTime">Cron Time</TableHeaderColumn>
          <TableHeaderColumn dataField="desc" >Description</TableHeaderColumn>
          <TableHeaderColumn dataField="context" dataFormat={executionMaker}>Last Execution</TableHeaderColumn>
          <TableHeaderColumn dataField="context" dataFormat={runningMaker}>Status</TableHeaderColumn>
          <TableHeaderColumn dataField="" dataFormat={miniDetails.bind(this)}>Actions</TableHeaderColumn>
        </BootstrapTable>
      </div>
    );
  }
});
export default connect(state => state.workflow)(WorkflowScheduleList);
