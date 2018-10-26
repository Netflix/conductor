import React, {Component} from 'react';
import { Input, Button, ButtonToolbar, OverlayTrigger, Panel, Popover, Col, Row, Grid } from 'react-bootstrap';
import CronParser from "./CronParser";

class WorkflowMetaCron extends Component {
    constructor(props) {
        super(props);

        this.state = {
            setButton: true,
            cronExp: null,
            cronDesc: null,
            cronArr: Array(6).fill('*'),
        }
    }

    handleCron(idx, e) {
        let cronArr = this.state.cronArr;

        if (e.target.value === "") {
            cronArr.splice(idx, 1, "*");
        } else
            cronArr.splice(idx, 1, e.target.value);

        this.setState({ cronArr: cronArr });
        this.state.cronExp = cronArr.join(" ");

    };

    handleCronDesc(e) {
        this.state.cronDesc = e.target.value;
    };

    render() {
        return (
            <div>
                <div className="input-grid">
                    <form>
                        <Grid>
                            <Row className="show-grid">
                                <Col md={1}>
                                    <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 0)}/>
                                    &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;&nbsp;<label className="small nobold">Seconds</label>
                                </Col>
                                <Col md={1}>
                                    <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 1)}/>
                                    &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;&nbsp;&nbsp;<label className="small nobold">Minutes</label>
                                </Col>
                                <Col md={1}>
                                    <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 2)}/>
                                    &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<label className="small nobold">Hours</label>
                                </Col>
                                <Col md={1}>
                                    <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 3)}/>
                                    &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;&nbsp;<label className="small nobold">Day of Month</label>
                                </Col>
                                <Col md={1}>
                                    <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 4)}/>
                                    &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<label className="small nobold">Months</label>
                                </Col>
                                <Col md={1}>
                                    <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 5)}/>
                                    &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;&nbsp;<label className="small nobold">Day of Week</label>
                                </Col>

                                <ButtonToolbar>
                                        <OverlayTrigger trigger="click" rootClose placement="right" overlay={
                                                <Popover title="Cron Expression Help" width={500}>
                                                <Panel header="Expression">
                                                    <span className="small"><pre id="input">
                                                                                                                                      
                                                    </pre></span>
                                                </Panel>
                                                </Popover>
                                            }><Button bsStyle="info">
                                            <i className="fas fa-question-circle"/>&nbsp;&nbsp;Help</Button>
                                        </OverlayTrigger>
                                </ButtonToolbar>

                            </Row>
                        </Grid>
                    </form>
                </div>

                <Input type="input" placeholder="Description (optional)" onChange={this.handleCronDesc.bind(this)}/>
                 <br/>
                <CronParser cronArr={this.state.cronArr} ref="cronparser"/>

            </div>
        )
    }
}
export default WorkflowMetaCron;