import React, { Component } from 'react';
import moment from 'moment';
import { Link, browserHistory } from 'react-router';
import { Breadcrumb, BreadcrumbItem, Input, Well, Button, Panel, DropdownButton, Grid, ButtonToolbar, MenuItem, Popover, OverlayTrigger, ButtonGroup, Row, Col, Table } from 'react-bootstrap';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';
import Typeahead from 'react-bootstrap-typeahead';
import { connect } from 'react-redux';
import { getErrorDataList } from '../../../actions/WorkflowActions';
import http from '../../../core/HttpClient';

class ErrorDashboardDetails extends Component{

constructor(props) {
    super(props);
    this.state = {
      sys: {}
    };

    http.get('/api/sys/').then((data) => {
      this.state = {
        sys: data.sys
      };
      window.sys = this.state.sys;
    });
  }

 componentWillReceiveProps(nextProps) {
    if (this.props.hash != nextProps.hash) {
     const inputData = {
                     searchString : nextProps.params.searchString,
                     errorLookupId : nextProps.params.errorLookupId,
                     frmDate : nextProps.params.fromDate,
                     toDate: nextProps.params.toDate,
                     range: nextProps.params.range
                    };
        this.props.dispatch(getErrorDataList(inputData));
    }
  }

  shouldComponentUpdate(nextProps, nextState){
    if(nextProps.refetch){
      const inputData = {
                      searchString : nextProps.params.searchString,
                      errorLookupId : nextProps.params.errorLookupId,
                      frmDate : nextProps.params.fromDate,
                      toDate: nextProps.params.toDate,
                      range: nextProps.params.range
                     };
         this.props.dispatch(getErrorDataList(inputData));
      return false;
    }
    return true;
  }


  render() {
    var errorData = this.props.errorData;
    console.log(errorData.result);
    return (
      <div className="ui-content">
        <h1>Transcode job failed</h1>
         <BootstrapTable data={errorData.result} striped={true} hover={true} exportCSV={false} pagination={true}>
                <TableHeaderColumn dataField="workflowId" isKey={true} dataAlign="left" dataSort={true}>Workflow ID</TableHeaderColumn>
                <TableHeaderColumn dataField="orderId" dataSort={true} >Order ID</TableHeaderColumn>
                <TableHeaderColumn dataField="jobId" dataSort={true} >Job ID</TableHeaderColumn>
                <TableHeaderColumn dataField="rankingId" dataSort={true} >Ranking ID</TableHeaderColumn>
                <TableHeaderColumn dataField="startTime" dataSort={true} >Failure Time</TableHeaderColumn>
                <TableHeaderColumn dataField="completeError" dataSort={true} >Complete Error Message</TableHeaderColumn>
                </BootstrapTable>
      </div>
    );
  }
};
export default connect(state => state.workflow)(ErrorDashboardDetails);
