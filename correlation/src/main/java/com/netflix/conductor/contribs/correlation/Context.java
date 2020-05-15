package com.netflix.conductor.contribs.correlation;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by beimforz on 12/21/17.
 */
public class Context {
	private String version;

	@JsonProperty("sequence-no")
	private int sequenceno;

	private List<String> urns;

	public Context() {
		this.version = "v1";
		this.sequenceno = 0;
		this.urns = new ArrayList<>();
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public int getSequenceno() {
		return sequenceno;
	}

	public void setSequenceno(int sequenceno) {
		this.sequenceno = sequenceno;
	}

	public List<String> getUrns() {
		return urns;
	}

	public void setUrns(List<String> urns) {
		this.urns = urns;
	}

	public String getUrn(String prefix) {
		for (String urn : urns) {
			if (urn.startsWith(prefix))
				return urn;
		}
		return null;
	}

	@Override
	public String toString() {
		return "{" +
				"version='" + version + '\'' +
				", sequence-no=" + sequenceno +
				", urns=" + urns.stream().map(o -> "'" + o + "'").collect(Collectors.toList()) +
				'}';
	}
}
