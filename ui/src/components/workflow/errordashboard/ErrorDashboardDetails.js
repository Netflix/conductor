import React, {Component} from 'react';
import moment from 'moment';
import {Link} from 'react-router';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';
import {connect} from 'react-redux';
import {getErrorDataList} from '../../../actions/WorkflowActions';
import http from '../../../core/HttpClient';

class ErrorDashboardDetails extends Component {

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
                searchString: nextProps.params.searchString,
                errorLookupId: nextProps.params.errorLookupId,
                frmDate: nextProps.params.fromDate,
                toDate: nextProps.params.toDate,
                range: nextProps.params.range
            };
            this.props.dispatch(getErrorDataList(inputData));
        }
    }

    shouldComponentUpdate(nextProps, nextState) {
        if (nextProps.refetch) {
            const inputData = {
                searchString: nextProps.params.searchString,
                errorLookupId: nextProps.params.errorLookupId,
                frmDate: nextProps.params.fromDate,
                toDate: nextProps.params.toDate,
                range: nextProps.params.range
            };
            this.props.dispatch(getErrorDataList(inputData));
            return false;
        }
        return true;
    }

    render() {

     const options = {
             sizePerPageList: [ {
               text: '5', value: 5
             }, {
               text: '10', value: 10
             },
              {
                  text: '100', value: 100
              } ], // you can change the dropdown list for size per page
             sizePerPage: 100 , // which size per page you want to locate as default
             alwaysShowAllBtns: true,
             prePage: 'Prev', // Previous page button text
             nextPage: 'Next', // Next page button text
             firstPage: 'First', // First page button text
             lastPage: 'Last' // Last page button text
        };

        let dateTime = moment().format(moment.HTML5_FMT.DATETIME_LOCAL_SECONDS);
        function formatDate(cell, row) {
            let dt = moment(cell).toDate()
            if (dt == null || dt == '') {
                return '';
            }
            return new Date(dt).toLocaleString('en-US');
        };

        function linkMaker(cell, row) {
            return <Link to={`/workflow/id/${cell}`}>{cell}</Link>;
        };

        var errorData = this.props.errorData;
        return (
            <div className="ui-content">

                {(this.props.params.lookup !== 'undefined' && (<h1>{this.props.params.lookup}</h1>))}
                {errorData && errorData.result.length ?
                    <BootstrapTable data={errorData.result} striped={true} search={true} hover={true} exportCSV={true}
                                   csvFileName={"conductorErrorReport_"+dateTime+".csv"}  pagination={true}  options={ options }>
                        <TableHeaderColumn dataField="workflowId" isKey={true} dataFormat={linkMaker} dataAlign="left"
                                           dataSort={true}>Workflow ID</TableHeaderColumn>
                        <TableHeaderColumn dataField="subWorkflow" dataFormat={linkMaker} dataSort={true}>Sub Workflow</TableHeaderColumn>
                        <TableHeaderColumn dataField="orderId" dataSort={true}>Order ID</TableHeaderColumn>
                        <TableHeaderColumn dataField="jobId" dataSort={true}>Job ID</TableHeaderColumn>
                        <TableHeaderColumn dataField="rankingId" dataSort={true}>Ranking ID</TableHeaderColumn>
                        <TableHeaderColumn dataField="startTime" dataSort={true} dataFormat={formatDate}>Failure Time</TableHeaderColumn>
                        <TableHeaderColumn dataField="completeError" dataSort={true}>Complete Error Message</TableHeaderColumn>
                    </BootstrapTable> :
                    <i className="fa fa-spinner fa-spin" astyle={{fontSize: "100px", marginLeft: "50%", marginTop: "15%"}}></i>
                }
            </div>
        );
    }
};
export default connect(state => state.workflow)(ErrorDashboardDetails);
