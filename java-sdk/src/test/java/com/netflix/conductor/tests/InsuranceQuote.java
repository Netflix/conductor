package com.netflix.conductor.tests;

public class InsuranceQuote {

    private String name;

    private double quotedAmount;

    private double quotedPremium;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getQuotedAmount() {
        return quotedAmount;
    }

    public void setQuotedAmount(double quotedAmount) {
        this.quotedAmount = quotedAmount;
    }

    public double getQuotedPremium() {
        return quotedPremium;
    }

    public void setQuotedPremium(double quotedPremium) {
        this.quotedPremium = quotedPremium;
    }
}
