package com.fluidops.fedx;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;

public class Util {
	public static Object instantiate(String className, Object... args) {
		try {
			Class<?> cls = Class.forName(className);
			main_loop: for (Constructor<?> ctor : cls.getConstructors()) {
				if (ctor.getParameterCount() != args.length) continue;
				Parameter ps[] = ctor.getParameters();
				for (int pidx = 0; pidx < ps.length; ++pidx) {
					if (!ps[pidx].getType().isAssignableFrom(args[pidx].getClass())) continue main_loop;
				}
				return ctor.newInstance(args);
			}
			throw new NoSuchMethodException("The Expected constructor s not found for Class " + className);
		} catch (InstantiationException e) {
			throw new IllegalStateException("Class " + className + " could not be instantiated.", e);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Class " + className + " could not be found, check whether the name is correct.", e);
		} catch (Exception e) {
			throw new IllegalStateException("Unexpected error while instantiating " + className + ":", e);
		}
	}
}
