import React, {Component} from 'react';
import {Button, ButtonGroup, OverlayTrigger, Popover, Panel, Input, Grid, Row, Col, Tooltip} from 'react-bootstrap';
import {connect} from 'react-redux';
import {
    terminateWorkflow,
    cancelWorkflow,
    restartWorfklow,
    retryWorfklow,
    pauseWorfklow,
    resumeWorfklow,
    getWorkflowDetails
} from '../../../actions/WorkflowActions';

const WorkflowAction = React.createClass({

    getInitialState() {
        return {
            terminating: false,
            cancelling: false,
            rerunning: false,
            restarting: false,
            retrying: false,
            pausing: false,
            resuming: false
        };
    },

    render() {
        const tt_term = (
            <Popover id="popover-trigger-hover-focus" title="Terminate Workflow">
                Terminate workflow execution. All running tasks will be cancelled.
            </Popover>
        );
        const tt_cancel = (
            <Popover id="popover-trigger-hover-focus" title="Cancel Workflow">
                Cancel workflow execution. All running tasks will be cancelled.
            </Popover>
        );
        const tt_restart = (
            <Popover id="popover-trigger-hover-focus" title="Restart Workflow">
                Restart the workflow from the begining (First Task)
            </Popover>
        );
        const tt_retry = (
            <Popover id="popover-trigger-hover-focus" title="Retry Last Failed Task">
                Retry the last failed task and put workflow in running state
            </Popover>
        );
        const tt_pause = (
            <Popover id="popover-trigger-hover-focus" title="Pause Workflow">
                Pauses workflow execution. No new tasks will be scheduled until workflow has been resumed.
            </Popover>
        );
        const tt_resume = (
            <Popover id="popover-trigger-hover-focus" title="Resume Workflow">
                Resume workflow execution
            </Popover>
        );


        let is_admin_role = this.props.user.primary_role === 'ADMIN';
        let terminating = this.props.terminating;
        let cancelling = this.props.cancelling;
        let rerunning = this.state.rerunning;
        let restarting = this.props.restarting;
        let retrying = this.props.retrying;
        let pausing = this.props.pausing;
        let resuming = this.props.resuming;

        if (this.props.workflowStatus == 'RUNNING') {
            if (is_admin_role) {
                return (
                    <ButtonGroup>
                        <OverlayTrigger placement="bottom" overlay={tt_term}>
                            <Button
                                bsStyle="danger" bsSize="xsmall" disabled={terminating}
                                onClick={!terminating ? this.terminate : null}> {terminating ? (
                                <i className="fa fa-spinner fa-spin"></i>) : 'Terminate'}
                            </Button>
                        </OverlayTrigger>
                        <OverlayTrigger placement="bottom" overlay={tt_cancel}>
                            <Button
                                bsStyle="danger" bsSize="xsmall" disabled={cancelling}
                                onClick={!cancelling ? this.cancel : null}> {cancelling ? (
                                <i className="fa fa-spinner fa-spin"></i>) : 'Cancel'}
                            </Button>
                        </OverlayTrigger>
                        <OverlayTrigger placement="bottom" overlay={tt_pause}>
                            <Button
                                bsStyle="warning" bsSize="xsmall" disabled={pausing}
                                onClick={!pausing ? this.pause : null}> {pausing ? (
                                <i className="fa fa-spinner fa-spin"></i>) : 'Pause'}
                            </Button>
                        </OverlayTrigger>
                    </ButtonGroup>

                );
            } else {
                return (
                    <ButtonGroup>
                    </ButtonGroup>
                );
            }
        }
        if (this.props.workflowStatus == 'COMPLETED' || this.props.workflowStatus == 'CANCELLED') {
            if (is_admin_role) {
                return (
                    <OverlayTrigger placement="bottom" overlay={tt_restart}>
                        <Button
                            bsStyle="default" bsSize="xsmall" disabled={restarting}
                            onClick={!restarting ? this.restart : null}> {restarting ? (
                            <i className="fa fa-spinner fa-spin"></i>) : 'Restart'}
                        </Button>
                    </OverlayTrigger>
                );
            } else {
                return (
                    <ButtonGroup>
                    </ButtonGroup>
                );
            }
        } else if (this.props.workflowStatus == 'FAILED' || this.props.workflowStatus == 'TERMINATED' || this.props.workflowStatus == 'RESET' || this.props.workflowStatus == 'FAILED_NO_RETRY') {
            if (is_admin_role) {
                return (
                    <ButtonGroup>
                        <OverlayTrigger placement="bottom" overlay={tt_restart}>
                            <Button
                                bsStyle="default" bsSize="xsmall" disabled={restarting}
                                onClick={!restarting ? this.restart : null}> {restarting ? (
                                <i className="fa fa-spinner fa-spin"></i>) : 'Restart'}
                            </Button>
                        </OverlayTrigger>
                        <OverlayTrigger placement="bottom" overlay={tt_retry}>
                            <Button
                                bsStyle="default" bsSize="xsmall" disabled={retrying}
                                onClick={!retrying ? this.retry : null}> {retrying ? (
                                <i className="fa fa-spinner fa-spin"></i>) : 'Retry'}
                            </Button>
                        </OverlayTrigger>
                    </ButtonGroup>
                );
            } else {
                return (
                    <ButtonGroup>
                    </ButtonGroup>
                );
            }
        } else if (this.props.workflowStatus == 'PAUSED') {
            if (is_admin_role) {
                return (
                    <ButtonGroup>
                        <OverlayTrigger placement="bottom" overlay={tt_resume}>
                            <Button
                                bsStyle="success" bsSize="xsmall" disabled={resuming}
                                onClick={!resuming ? this.resume : null}> {resuming ? (
                                <i className="fa fa-spinner fa-spin"></i>) : 'Resume'}
                            </Button>
                        </OverlayTrigger>
                    </ButtonGroup>
                );
            } else {
                return (
                    <ButtonGroup>
                    </ButtonGroup>
                );
            }
        } else {
            return (
                <ButtonGroup>
                    <OverlayTrigger placement="bottom" overlay={tt_restart}>
                        <Button
                            bsStyle="default" bsSize="xsmall" disabled={restarting}
                            onClick={!restarting ? this.restart : null}> {restarting ? (
                            <i className="fa fa-spinner fa-spin"></i>) : 'Restart'}
                        </Button>
                    </OverlayTrigger>
                </ButtonGroup>
            );

        }
    },
    terminate() {
        this.setState({terminating: true});
        this.props.dispatch(terminateWorkflow(this.props.workflowId));
    },
    cancel() {
        this.setState({cancelling: true});
        this.props.dispatch(cancelWorkflow(this.props.workflowId));
    },
    rerun() {
        this.setState({rerunning: true});
    },
    restart() {
        this.setState({restarting: true});
        this.props.dispatch(restartWorfklow(this.props.workflowId));
    },
    retry() {
        this.setState({retrying: true});
        this.props.dispatch(retryWorfklow(this.props.workflowId));
    },
    pause() {
        this.setState({pausing: true});
        this.props.dispatch(pauseWorfklow(this.props.workflowId));
    },
    resume() {
        this.setState({resuming: true});
        this.props.dispatch(resumeWorfklow(this.props.workflowId));
    }
});

export default connect(state => state.global)(WorkflowAction);
