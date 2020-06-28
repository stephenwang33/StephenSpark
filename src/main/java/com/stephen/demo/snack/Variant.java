package com.stephen.demo.snack;

public class Variant {
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getOption1() {
		return option1;
	}
	public void setOption1(String option1) {
		this.option1 = option1;
	}
	public String getOption2() {
		return option2;
	}
	public void setOption2(String option2) {
		this.option2 = option2;
	}
	public String getOption3() {
		return option3;
	}
	public void setOption3(String option3) {
		this.option3 = option3;
	}
	public String getSku() {
		return sku;
	}
	public void setSku(String sku) {
		this.sku = sku;
	}
	public Boolean getRequires_shipping() {
		return requires_shipping;
	}
	public void setRequires_shipping(Boolean requires_shipping) {
		this.requires_shipping = requires_shipping;
	}
	public Boolean getTaxable() {
		return taxable;
	}
	public void setTaxable(Boolean taxable) {
		this.taxable = taxable;
	}
	public Image getFeatured_image() {
		return featured_image;
	}
	public void setFeatured_image(Image featured_image) {
		this.featured_image = featured_image;
	}
	public Boolean getAvailable() {
		return available;
	}
	public void setAvailable(Boolean available) {
		this.available = available;
	}
	public String getPrice() {
		return price;
	}
	public void setPrice(String price) {
		this.price = price;
	}
	public String getGrams() {
		return grams;
	}
	public void setGrams(String grams) {
		this.grams = grams;
	}
	public String getCompare_at_price() {
		return compare_at_price;
	}
	public void setCompare_at_price(String compare_at_price) {
		this.compare_at_price = compare_at_price;
	}
	public Long getPosition() {
		return position;
	}
	public void setPosition(Long position) {
		this.position = position;
	}
	public Long getProduct_id() {
		return product_id;
	}
	public void setProduct_id(Long product_id) {
		this.product_id = product_id;
	}
	public String getCreated_at() {
		return created_at;
	}
	public void setCreated_at(String created_at) {
		this.created_at = created_at;
	}
	public String getUpdated_at() {
		return updated_at;
	}
	public void setUpdated_at(String updated_at) {
		this.updated_at = updated_at;
	}
	private Long id;
	private String title;
	private String option1;
	private String option2;
	private String option3;
	private String sku;
	private Boolean requires_shipping;
	private Boolean taxable;
	private Image featured_image;	private Boolean available;
	private String price;
	private String grams;
	private String compare_at_price;
	private Long position;
	private Long product_id;
	private String created_at;
	private String updated_at;

}
