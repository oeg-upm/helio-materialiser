package helio.materialiser.data;

import java.io.InputStream;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Method;

import com.google.gson.JsonObject;

import helio.framework.materialiser.mappings.DataProvider;

public class PlugableClass implements DataProvider {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6661355835924243132L;
	private Class<? extends DataProvider> clazz;
	private Object instance;
	
	public PlugableClass(Class<? extends DataProvider> clazz, Object instance) {
		this.clazz = clazz;
		this.instance = instance;
	}
	
	@Override
	public void configure(JsonObject arg0) {
		try {
			Method method = clazz.getMethod("configure");
			method.invoke(instance, arg0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public InputStream getData() {
		InputStream out = null;
		try {
			//Method method = clazz.getMethod("getData");
			//out = (InputStream) method.invoke(instance, "");
			//Method[] methods = instance.getClass().getDeclaredMethods();
			AnnotatedType interfaces = instance.getClass().getAnnotatedSuperclass();
			System.out.println(interfaces.getClass());
			Method[] methods = instance.getClass().getMethods(); 
	        // Printing method names 
	        for (Method method:methods) 
	            System.out.println(method.getName()); 
		} catch (Exception e) {
			e.printStackTrace();
		}
		return out;
	}
	
	

}
