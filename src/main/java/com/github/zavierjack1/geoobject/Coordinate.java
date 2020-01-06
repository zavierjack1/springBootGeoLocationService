package com.github.zavierjack1.geoobject;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Coordinate {
	private double longitude;
	private double latitude;
	
	public Coordinate(
			@JsonProperty("longitude") final double longitude, 
			@JsonProperty("latitude")final double latitude) {
		super();
		this.longitude = longitude;
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}
	
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public static double distance(Coordinate a, Coordinate b,  String unit) {
		if ((a.getLatitude() == b.getLatitude()) && (a.getLongitude() == b.getLongitude())) {
			return 0;
		}
		else {
			double theta = a.getLongitude() - b.getLongitude();
			double dist = Math.sin(Math.toRadians(a.getLatitude())) * Math.sin(Math.toRadians(b.getLatitude())) + Math.cos(Math.toRadians(a.getLatitude())) * Math.cos(Math.toRadians(b.getLatitude())) * Math.cos(Math.toRadians(theta));
			dist = Math.acos(dist);
			dist = Math.toDegrees(dist);
			dist = dist * 60 * 1.1515;
			if (unit.equals("K")) {
				dist = dist * 1.609344;
			} else if (unit.equals("N")) {
				dist = dist * 0.8684;
			}
			return (dist);
		}
	}
	
    public String toString() {
    	String str= "";
    	try {
    		str = new ObjectMapper().writeValueAsString(this);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return str;
    }
}
