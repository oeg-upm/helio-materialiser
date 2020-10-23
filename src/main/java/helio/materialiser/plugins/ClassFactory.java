package helio.materialiser.plugins;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.AnnotatedType;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;



public class ClassFactory {

	private File jarFile;
	private URLClassLoader classLoader;
	private Logger log = Logger.getLogger(ClassFactory.class.getName());

	
	public ClassFactory(String jarFileName) {
		try {
			jarFile = new File(jarFileName);
			// 1. Create a loader
			URL[] jarURLs = {new URL("file://"+jarFile.getCanonicalPath())};
			classLoader = URLClassLoader.newInstance(jarURLs);
		}catch(Exception e) {
			log.severe("An error ocurred building the Plugin loader");
			log.severe(e.toString());
		}
	}
	
	public Class<?> getClassByFullName(String className){
		Class<?> clazz = null;
		try {
			clazz = classLoader.loadClass(className);
		}catch(NoClassDefFoundError | ClassNotFoundException e) {
			log.severe("An error ocurred looking for the class: "+className);
			log.severe(e.toString());
		}
		return clazz;
	}
	
	public Class<?> getClassByName(String className){
		Class<?> clazz = null;
		List<String> classes = classesInJar();
		int classesSize = classes.size();
		for (int index = 0; index < classesSize; index++) {
			String clazzFoundName = classes.get(index);
			try {
				if(clazzFoundName.endsWith("."+className)) {
					clazz = classLoader.loadClass(clazzFoundName);
					if(clazz!=null)
						break;
				}
			} catch (NoClassDefFoundError | ClassNotFoundException e) {
				//e.printStackTrace(); this should be like this tp avoid printing verbose exceptions that may not refer to the class we are looking for
			}
		}
		return clazz;
	}
	
	public List<Class<?>> findClassesByInterface(String interfaceName) {
		List<Class<?>> result = new ArrayList<>();
		List<String> classes = classesInJar();
		int classesSize = classes.size();
		for (int index = 0; index < classesSize; index++) {
			String className = classes.get(index);
			try {
				Class<?> clazz = classLoader.loadClass(className);
				for (AnnotatedType clazzInterface : clazz.getAnnotatedInterfaces()) {
					String clazzInterfaceName = clazzInterface.getType().toString().replaceAll("^interface\\s*", "");
					if(clazzInterfaceName.equals(interfaceName))
						result.add(clazz);
				}
			} catch (NoClassDefFoundError | ClassNotFoundException e) {
				//e.printStackTrace(); this should be like this tp avoid printing verbose exceptions that may not refer to the class we are looking for
			}
		}
		return result;
	}
			
	private List<String> classesInJar() {
		List<String> classNames = new ArrayList<>();
		FileInputStream file = null;
		ZipInputStream zip = null;
		try {
			file = new FileInputStream(jarFile.getCanonicalPath());
			zip = new ZipInputStream(file);
			for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
				if (!entry.isDirectory() && entry.getName().endsWith(".class")) {
					// This ZipEntry represents a class. Now, what class does it represent?
					String className = entry.getName().replace('/', '.'); // including ".class"
					classNames.add(className.substring(0, className.length() - ".class".length()));
				}
			}
		} catch (Exception e) {
			log.severe("An error ocurred discovering the classes in the provided jar: "+jarFile.getName());
		} finally {
			try {
				if (file != null)
					file.close();
				if (zip != null)
					zip.close();
			} catch (Exception e) {
				log.severe("An error ocurred closing the jar file: "+jarFile.getName());
			}
		}
		return classNames;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((jarFile == null) ? 0 : jarFile.hashCode());
		return result;
	}

	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ClassFactory other = (ClassFactory) obj;
		if (jarFile == null) {
			if (other.jarFile != null)
				return false;
		} else if (!jarFile.equals(other.jarFile))
			return false;
		return true;
	}

	
}