
public class Product {
	private String productName;
	private String amount;
	
	private String price;
	
	
	
	public Product(String productName, String amount, String price) {
		super();
		this.productName = productName;
		this.amount = amount;
		this.price = price;
	}
	public String getProductName() {
		return productName;
	}
	public void setProductName(String productName) {
		this.productName = productName;
	}
	public String getAmount() {
		return amount;
	}
	public void setAmount(String amount) {
		this.amount = amount;
	}
	public String getPrice() {
		return price;
	}
	public void setPrice(String price) {
		this.price = price;
	}
	
	@Override
	public String toString() {
		return "Product [productName=" + productName + ", amount=" + amount + ", price=" + price + "]";
	}
	
	
}
