package com.sebis.mobility.model;

/**
 * Created by sohaib on 30/03/17.
 */
public class Travel {

	private int origin;
	private int destination;
	private String info = "";

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public int getOrigin() {
		return origin;
	}

	public void setOrigin(int origin) {
		this.origin = origin;
	}

	public int getDestination() {
		return destination;
	}

	public void setDestination(int destination) {
		this.destination = destination;
	}
}
