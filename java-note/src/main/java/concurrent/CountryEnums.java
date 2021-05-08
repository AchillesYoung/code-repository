package concurrent;

public enum CountryEnums
{
	ONE(1,"韩"),TWO(2,"魏"),THREE(3,"赵"),FOUR(4,"齐"),FIVE(5,"楚"),SIX(6,"燕");
	// mysql
	// ID	col1 col2 col3 col4
	//ONE(k,v1,v2,v3,v4)
	
	private Integer retCode;
	private String  retMessage;
	
	private CountryEnums(Integer retCode, String retMessage)
	{
		this.retCode = retCode;
		this.retMessage = retMessage;
	}

	public Integer getRetCode()
	{
		return retCode;
	}

	public void setRetCode(Integer retCode)
	{
		this.retCode = retCode;
	}

	public String getRetMessage()
	{
		return retMessage;
	}

	public void setRetMessage(String retMessage)
	{
		this.retMessage = retMessage;
	}
	
	public static CountryEnums forEachCountryEnums(Integer index)
	{
		for (CountryEnums element : values()) 
		{
			if(element.getRetCode() == index)
			{
				return element;
			}
		}
		return null;
	}
	
	
	
}







