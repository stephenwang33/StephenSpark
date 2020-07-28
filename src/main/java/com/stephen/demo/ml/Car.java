package com.stephen.demo.ml;

public class Car {
	public String getMaker() {
		return maker;
	}
	public void setMaker(String maker) {
		this.maker = maker;
	}
	public String getFuelType() {
		return fuelType;
	}
	public void setFuelType(String fuelType) {
		this.fuelType = fuelType;
	}
	public String getAspire() {
		return aspire;
	}
	public void setAspire(String aspire) {
		this.aspire = aspire;
	}
	public String getDoors() {
		return doors;
	}
	public void setDoors(String doors) {
		this.doors = doors;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	public String getDriver() {
		return driver;
	}
	public void setDriver(String driver) {
		this.driver = driver;
	}
	public String getCyclinders() {
		return cyclinders;
	}
	public void setCyclinders(String cyclinders) {
		this.cyclinders = cyclinders;
	}
	public Integer getHP() {
		return hp;
	}
	public void setHP(Integer hp) {
		this.hp = hp;
	}
	public Integer getRPM() {
		return rpm;
	}
	public void setRPM(Integer rpm) {
		this.rpm = rpm;
	}
	public Double getMPGCity() {
		return mpgCity;
	}
	public void setMPGCity(Double mpgCity) {
		this.mpgCity = mpgCity;
	}
	public Double getMPGHighWay() {
		return mpgHighWay;
	}
	public void setMPGHighWay(Double mpgHighWay) {
		this.mpgHighWay = mpgHighWay;
	}
	public Double getPrice() {
		return price;
	}
	public void setPrice(Double price) {
		this.price = price;
	}
	
	private String maker;
	private String fuelType;
	private String aspire;
	private String doors;
	private String body;
	private String driver;
	private String cyclinders;
	private Integer hp;
	private Integer rpm;
	private Double mpgCity;
	private Double mpgHighWay;
	private Double price;
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		sb.append("Maker:");
		sb.append(this.maker);
		sb.append(", ");
		sb.append("FuelType:");
		sb.append(this.fuelType);
		sb.append(", ");
		sb.append("Aspire:");
		sb.append(this.aspire);
		sb.append(", ");
		sb.append("Doors:");
		sb.append(this.doors);
		sb.append(", ");
		sb.append("Bpdy:");
		sb.append(this.body);
		sb.append(", ");
		sb.append("Driver:");
		sb.append(this.driver);
		sb.append(", ");
		sb.append("Cyclinders:");
		sb.append(this.cyclinders);
		sb.append(", ");

		sb.append("HP:");
		sb.append(this.hp);
		sb.append(", ");
		sb.append("RPM:");
		sb.append(this.rpm);
		sb.append(", ");
		sb.append("MPG-CITY:");
		sb.append(this.mpgCity);
		sb.append(", ");
		sb.append("MPG-HWY:");
		sb.append(this.mpgHighWay);
		sb.append(", ");
		sb.append("Price:");
		sb.append(this.price);
		sb.append("}");
		return sb.toString();
	}
	
}


