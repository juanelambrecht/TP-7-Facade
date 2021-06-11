package ar.unrn;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;



public class DBFacadeImplements implements DBFacade {

	private Connection conexion;

	@Override
	public void open() {

		Properties propiedades = new Properties();

		InputStream entrada = null;

		try {
			entrada = new FileInputStream("datos.properties");

			propiedades.load(entrada);

			Class.forName(propiedades.getProperty("CONTROLADOR"));
			conexion = DriverManager.getConnection(propiedades.getProperty("URL"), propiedades.getProperty("USUARIO"),
					propiedades.getProperty("CONTRASEÑA"));

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Map<String, String>> queryResultAsAsociation(String sql) {

		List<Map<String, String>> listMap = new ArrayList<Map<String, String>>();

		this.open();

		try {

			Statement sent = this.conexion.createStatement();

			ResultSet resul = sent.executeQuery(sql);
			ResultSetMetaData rsmd = resul.getMetaData();
	
			String columnaID = rsmd.getColumnName(1);
			String clave = rsmd.getColumnName(2);
			
			while (resul.next()) {

				Map<String, String> elemento = new HashMap<String, String>();
				
				elemento.put(resul.getString(columnaID),resul.getString(clave));
				
				listMap.add(elemento);

			}

		this.close();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return listMap;
	}

	@Override
	public List<String[]> queryResultAsArray(String sql) {
		this.open();

		try {

			Statement sent = this.conexion.createStatement();

			ResultSet resul = sent.executeQuery(sql);
			
			ResultSetMetaData rsmd = resul.getMetaData();
			
			String clave = rsmd.getColumnName(2);
			
			while (resul.next()) {

				List<String[]> list = new ArrayList<String[]>();
				String[] addressesArr  = new String[1];

				addressesArr[0] = resul.getString(clave);
				
				list.add(addressesArr);
			}

		this.close();

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	@Override
	public void close() {
		try {
			conexion.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
