import React, { Component } from 'react';
import moment from 'moment';
import { Link, browserHistory } from 'react-router';
import { Breadcrumb, BreadcrumbItem, Input, Well, Button, Panel, DropdownButton, Grid, ButtonToolbar, MenuItem, Popover, OverlayTrigger, ButtonGroup, Row, Col, Table } from 'react-bootstrap';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';
import Typeahead from 'react-bootstrap-typeahead';
import { connect } from 'react-redux';
import { getErrorData,getErrorCountDay, getErrorCountWeek, getErrorDataMonth } from '../../../actions/WorkflowActions';

const ErrorDashboard = React.createClass({

  getInitialState() {
    return {
      name: '',
      version: '',
      errorData: [],
      errorDataDay: [],
      errorDataWeek: [],
      errorDataMonth: []
    }
  },

  componentWillMount(){
     const inputData = {
           searchString :this.state.search,
           frmDate :this.state.frmDate,
           toDate: this.state.toDate
          };

        this.props.dispatch(getErrorData(inputData));
        this.props.dispatch(getErrorCountDay(inputData));
        this.props.dispatch(getErrorCountWeek(inputData));
        this.props.dispatch(getErrorDataMonth(inputData));
  },

  componentWillReceiveProps(nextProps){
    this.state.errorData = nextProps.errorData;
    this.state.errorDataDay = nextProps.errorDataDay;
    this.state.errorDataWeek = nextProps.errorDataWeek;
    this.state.errorDataMonth = nextProps.errorDataMonth;
  },

  searchChange(e){
      let val = e.target.value;
      this.setState({ search: val });
    },
    dateChangeFrom(e){
      this.setState({ datefrm: e.target.value });
    },
    dateChangeTo(e){
         this.setState({ dateto: e.target.value });
      },

 rangeChange(range) {
     if (range != null && range.length > 0) {
       let value = range[range.length - 1];
       this.setState({ range: [value] });
       this.state.range = [value];
     } else {
       this.setState({ range: [] });
     }
   },

  clearBtnClick() {
   this.setState({
     datefrm:"",
     dateto: ""
   });
 },

 searchBtnClick() {
        const inputData = {
                 searchString : this.state.search,
                 frmDate : this.state.datefrm,
                 toDate : this.state.dateto,
                 range : this.state.range
                };
              this.props.dispatch(getErrorData(inputData));
      },
  render() {
    var errorData = this.state.errorData;
    var errorDataDay = this.state.errorDataDay;
    var errorDataWeek = this.state.errorDataWeek;
    var errorDataMonth = this.state.errorDataMonth;

    var dayErrorCount = 0 ;
    var weekErrorCount = 0 ;
    var monthErrorCount = 0 ;
     if (errorDataDay !== undefined && errorDataDay.result !== undefined ) {
      errorDataDay.result.forEach(function (d) {
         if(d.isRequiredInReporting == true || (d.isRequiredInReporting == false && d.id === 0)){
         dayErrorCount=dayErrorCount+parseInt(d.totalCount);
         }
        });
      }
      if (errorDataWeek !== undefined && errorDataWeek.result !== undefined ) {
            errorDataWeek.result.forEach(function (d) {
               if(d.isRequiredInReporting == true  || (d.isRequiredInReporting == false && d.id === 0)){
               weekErrorCount=weekErrorCount+parseInt(d.totalCount);
               }
              });
      }
      if (errorDataMonth !== undefined && errorDataMonth.result !== undefined ) {
                  errorDataMonth.result.forEach(function (d) {
                     if(d.isRequiredInReporting == true  || (d.isRequiredInReporting == false && d.id === 0)){
                     monthErrorCount=monthErrorCount+parseInt(d.totalCount);
                 }
           });
       }
    var knownErrors = [];
    var unknownErrors = [];
      if (errorData !== undefined && errorData.result !== undefined ) {
          errorData.result.forEach(function (d) {

           if(d.id === 0)
           {
            unknownErrors.push({
              id: d.id,
              lookup: d.lookup,
              totalCount: d.totalCount
            });
           }
           else
           {
             if(d.isRequiredInReporting == true){
              knownErrors.push({
                        id: d.id,
                        lookup: d.lookup,
                        totalCount: d.totalCount
                       });
                }
           }
          });
        }
    const rangeList = ['All data','This year',
      'Last quarter','This quarter',
      'Last month','This month',
      'Yesterday', 'Today',
      'Last 30 minutes', 'Last 5 minutes'];
    const workflowNames = this.state.workflows?this.state.workflows:[];

    return (
      <div className="ui-content">
        <h1>Workflow Error Dashboard</h1>
         <Panel header="Filter Workflows Errors">
          <Grid fluid={true}>
                     <Row className="show-grid">
                       <Col md={2}>
                         <Typeahead ref="range" onChange={this.rangeChange} options={rangeList} placeholder="Today by default" selected={this.state.range} multiple={true} disabled={this.state.h}/>
                         &nbsp;<i className="fa fa-angle-up fa-1x"></i>&nbsp;&nbsp;<label className="small nobold">Filter by date range</label>
                       </Col>
                       <Col md={4}>
                         <Input type="input" placeholder="Search" groupClassName="" ref="search" value={this.state.search} labelClassName="" onKeyPress={this.keyPress} onChange={this.searchChange}/>
                         &nbsp;<i className="fa fa-angle-up fa-1x"></i>&nbsp;&nbsp;<label className="small nobold">Free Text Query</label>
                         &nbsp;&nbsp;<input type="checkbox" checked={this.state.fullstr} onChange={this.prefChange} ref="fullstr"/><label className="small nobold">&nbsp;Search for entire string</label>
                         </Col>
                       <Col md={5}>
                        <Button bsSize="small" bsStyle="success" onClick={this.clearBtnClick}>&nbsp;&nbsp;Clear date range</Button> &nbsp;&nbsp;
                                                          <Button bsSize="medium" bsStyle="success" onClick={this.searchBtnClick} className="fa fa-search search-label">&nbsp;&nbsp;Search</Button>
                       </Col>
                     </Row>
                      <Row className="show-grid">
                        <Col md={2}>
                           <input  name="datefrm"  type="date" value={this.state.datefrm} className="form-control"  onChange={ this.dateChangeFrom } />
                            &nbsp;<i className="fa fa-angle-up fa-1x"></i>&nbsp;&nbsp;<label className="small nobold">From Date</label>
                         </Col>
                         <Col md={2}>
                            <input  name="dateto"  type="date" value={this.state.dateto} className="form-control"  onChange={ this.dateChangeTo } />
                                &nbsp;<i className="fa fa-angle-up fa-1x"></i>&nbsp;&nbsp;<label className="small nobold">To Date</label>

                           </Col>
                             <Col md={3}>

                              </Col>
                      </Row>
                   </Grid>
         </Panel>
           <Panel header="Total Error Counts">
            <Table striped bordered hover>
                             <thead>
                                 <tr>
                                 <th>Today</th>
                                 <th>This Week</th>
                                 <th>This Month</th>
                                 </tr>
                               </thead>

                            <tbody>
                                <tr>
                                   <td>{dayErrorCount}</td>
                                   <td> {weekErrorCount}</td>
                                   <td> {monthErrorCount}</td>
                                </tr>
                            </tbody>

                          </Table>
           </Panel>
          <Panel header="Unknown Errors">
              <Table striped bordered hover>
                  <thead>
                      <tr>
                      <th>Error Type</th>
                      <th>Total Count</th>
                      </tr>
                    </thead>
                  {unknownErrors !== undefined && unknownErrors.map(item=>(
                 <tbody>
                     <tr>
                        <td> <Link to={'/workflow/errorDashboard/details/'+item.id+'/'+this.state.search+'/'+this.state.datefrm+'/'+this.state.dateto+'/'+this.state.range+'/'+item.lookup }>Unknown Error</Link></td>
                        <td><label className="small nobold">{item.totalCount} </label><br/></td>
                     </tr>
                 </tbody>
                ))}
               </Table>
            </Panel>
           <Panel header="Known Errors">
           <Table striped bordered hover>
              <thead>
                <tr>
                  <th>Error Type</th>
                   <th>Total Count</th>
                </tr>
               </thead>
               {knownErrors !== undefined && knownErrors.map(item=>(
                <tbody>
                 <tr>
                 <td> <Link to={'/workflow/errorDashboard/details/'+item.id+'/'+this.state.search+'/'+this.state.datefrm+'/'+this.state.dateto+'/'+this.state.range+'/'+item.lookup }>{item.lookup}</Link></td>
                 <td><label className="small nobold">{item.totalCount} </label><br/></td>
                 </tr>
                 </tbody>
                ))}
                 </Table>
            </Panel>

      </div>
    );
  }
});
export default connect(state => state.workflow)(ErrorDashboard);
