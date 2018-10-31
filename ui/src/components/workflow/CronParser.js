import React, {Component} from 'react';

class CronParser extends Component {
    constructor(props) {
        super(props);

        this.state = {
            cronArr: this.props.cronArr,
            at: "at",
            seconds: "",
            minutes: "",
            hours: "",
            dayofmonth: "",
            month: "",
            dayofweek: ""
        }
    }

    componentWillReceiveProps(nextProps){
        this.setState({
            cronArr: nextProps.cronArr,
        }, () => {
            this.cronParser(this.state.cronArr);
        });
    }

    cronParser(cronArr){

        //seconds
        if(cronArr[0] === "*"){
            this.setState({ seconds: " every second" })
        }
        if(cronArr[0].match(/^[0-9]+/)){

            if(cronArr[1].match(/^[0-9]+/) && cronArr[2].match(/^[0-9]+/)){
                if(cronArr[0].match(/^[0-9]$/)){
                    this.setState({ hours: "0" + cronArr[0] })
                } else
                    this.setState({ hours: cronArr[0] })
            } else
                this.setState({ seconds: " second " + cronArr[0] })

        }
        if(cronArr[0].startsWith("*/")){
            let num = cronArr[0].substr(2,cronArr[0].length - 1);
            this.setState({ seconds: " every " + num + "th second" })
        }
        if(cronArr[0].includes("-")){
            this.setState({ seconds: " every second from " + cronArr[0] })
        }
        if(cronArr[0].includes(",")){
            this.setState({ seconds: " second " + cronArr[0] })
        }

        //minutes
        if(cronArr[1] === "*"){
            this.setState({ minutes: "" })
        }
        if(cronArr[1].match(/^[0-9]+/)){
            if(cronArr[0].match(/^[0-9]+/) && cronArr[2].match(/^[0-9]+/)){
                if(cronArr[1].match(/^[0-9]$/)){
                    this.setState({ minutes: "0" + cronArr[1] + ":" })
                } else
                    this.setState({ minutes: cronArr[1] + ":" })
            } else
                this.setState({ minutes: " past minute " + cronArr[1] })
        }
        if(cronArr[1].startsWith("*/")){
            let num = cronArr[1].substr(2,cronArr[1].length - 1);
            this.setState({ minutes: " past every " + num + ". minute" })
        }
        if(cronArr[1].includes("-")){
            this.setState({ minutes: " past every minute from " + cronArr[1] })
        }
        if(cronArr[1].includes(",")){
            this.setState({ minutes: " past minute " + cronArr[1] })
        }

        //hours
        if(cronArr[2] === "*"){
            this.setState({ hours: "" })
        }
        if(cronArr[2].match(/^[0-9]+/)){
            if(cronArr[0].match(/^[0-9]+/) && cronArr[1].match(/^[0-9]+/)){
                if(cronArr[2].match(/^[0-9]$/)){
                    this.setState({ seconds: " 0" + cronArr[2] + ":" })
                } else
                    this.setState({ seconds: " " + cronArr[2] + ":" })
            } else
                this.setState({ hours: " past hour " + cronArr[2] })
        }
        if(cronArr[2].startsWith("*/")){
            let num = cronArr[2].substr(2,cronArr[2].length - 1);
            this.setState({ hours: " past every " + num + ". hour" })
        }
        if(cronArr[2].includes("-")){
            this.setState({ hours: " past every hour from " + cronArr[2] })
        }
        if(cronArr[2].includes(",")){
            this.setState({ hours: " past hour " + cronArr[2] })
        }

        //day of month
        if(cronArr[3] === "*"){
            this.setState({ dayofmonth: "" })
        }
        if(cronArr[3].match(/^[0-9]+/)){
            this.setState({ dayofmonth: " on day-of-month " + cronArr[3] })
        }
        if(cronArr[3].startsWith("*/")){
            let num = cronArr[3].substr(2,cronArr[3].length - 1);
            this.setState({ dayofmonth: " on every " + num + ". day-of-month" })
        }
        if(cronArr[3].includes("-")){
            this.setState({ dayofmonth: " on every day-of-month from " + cronArr[3] })
        }
        if(cronArr[3].includes(",")){
            this.setState({ dayofmonth: " past day-of-month " + cronArr[3] })
        }

        //month
        if(cronArr[4] === "*"){
            this.setState({ month: "" })
        }
        if(cronArr[4].match(/^[0-9]+/)){
            this.setState({ month: " in " + this.getMonth(cronArr[4]) })
        }
        if(cronArr[4].startsWith("*/")){
            let num = cronArr[4].substr(2,cronArr[4].length - 1);
            this.setState({ month: " in every " + this.getMonth(num) })
        }
        if(cronArr[4].includes("-")){
            this.setState({ month: " in every month from " + cronArr[4] })
        }
        if(cronArr[4].includes(",")){
            this.setState({ month: " in " + cronArr[4] })
        }

        //day of week
        if(cronArr[5] === "*"){
            this.setState({ dayofweek: "" })
        }
        if(cronArr[3] !== "*"){
            if(cronArr[5].match(/^[0-9]+/)){
                this.setState({ dayofweek: " and on " + this.getDay(cronArr[5]) })
            }
            if(cronArr[5].startsWith("*/")){
                let num = cronArr[4].substr(2,cronArr[5].length - 1);
                this.setState({ dayofweek: " if it's on every " + this.getDay(num) })
            }
            if(cronArr[5].includes("-")){
                let num1 = cronArr[5].substr(0,1);
                let num2 = cronArr[5].substr(2,cronArr[5].length - 1);
                this.setState({ dayofweek: " and on every day-of-week from " + this.getDay(num1) + " through " + this.getDay(num2) })
            }
            if(cronArr[5].includes(",")){
                this.setState({ dayofweek: " and on " + cronArr[5] })
            }
        }
        else {
            if(cronArr[5].match(/^[0-9]+/)){
                this.setState({ dayofweek: " on " + this.getDay(cronArr[5]) })
            }
            if(cronArr[5].startsWith("*/")){
                let num = cronArr[5].substr(2,cronArr[5].length - 1);
                this.setState({ dayofweek: " on every " + this.getDay(num) })
            }
            if(cronArr[5].includes("-")){
                let num1 = cronArr[5].substr(0,1);
                let num2 = cronArr[5].substr(2,cronArr[5].length - 1);
                this.setState({ dayofweek: " on every day-of-week from " + this.getDay(num1) + " through " + this.getDay(num2) })
            }
            if(cronArr[5].includes(",")){
                this.setState({ dayofweek: " on " + cronArr[5] })
            }
        }



    }

    getMonth(cronArr){

        if(cronArr === "0"){
            return "January"
        }
        if(cronArr === "1"){
            return "February"
        }
        if(cronArr === "2"){
            return "March"
        }
        if(cronArr === "3"){
            return "April"
        }
        if(cronArr === "4"){
            return "May"
        }
        if(cronArr === "5"){
            return "June"
        }
        if(cronArr === "6"){
            return "July"
        }
        if(cronArr === "7"){
            return "August"
        }
        if(cronArr === "8"){
            return "September"
        }
        if(cronArr === "9"){
            return "October"
        }
        if(cronArr === "10"){
            return "November"
        }
        if(cronArr === "11"){
            return "December"
        }
    }

    getDay(cronArr){

        if(cronArr === "0"){
            return "Monday"
        }
        if(cronArr === "1"){
            return "Tuesday"
        }
        if(cronArr === "2"){
            return "Wednesday"
        }
        if(cronArr === "3"){
            return "Thursday"
        }
        if(cronArr === "4"){
            return "Friday"
        }
        if(cronArr === "5"){
            return "Saturday"
        }
        if(cronArr === "6"){
            return "Sunday"
        }
    }

    render() {

        let { at, seconds, minutes, hours, dayofmonth, month, dayofweek } = this.state;

        return (

            <b>Workflow will execute {at}{seconds}{minutes}{hours}{dayofmonth}{month}{dayofweek}</b>

        )
    }
}
export default CronParser;