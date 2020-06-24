package model.dao;

import db.DB;
import model.dao.impl.DepartmentDaoJDBC;
import model.dao.impl.SellerDaoJDBC;

public class DaoFactory {
    
    public static SellerDao createSellerDao() {
	//isso aq � criado pra retornar um jdbc, mas caso eu queria mudar, colocar outro sem ser JDBC, eu s� mudo aqui e gg
	// like new SellerDaoMongoDB. saca? ai ja muda tudo automaticamente l�, xD izi. f�cil manuten��p demais
	return new SellerDaoJDBC(DB.getConnection());
    }
    
    public static DepartmentDao createDepartmentDao() {
	return new DepartmentDaoJDBC(DB.getConnection());
    }

}
