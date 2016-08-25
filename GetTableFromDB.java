package test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import oracle.spatial.geometry.JGeometry;
import oracle.sql.STRUCT;
import database.SqlDo;

public class GetTableFromDB {
	public static void main(String[] args) {
		String city = "深圳市";
		String lclass = "知名大学";
		CarryOut(city, lclass, 1);
//		CarryOut2(city, lclass, 1);
	}
	
	public static void CarryOut(String city, String lclass, int flag){
		String filepathString = city + lclass + "cons.tsv";
		extractData(city, lclass);
		System.out.println("表格书写...");
		WriteListToCSV.writeCSVFromList(list, city + lclass + ".tsv");
		
		if(flag == 0){
			if(!Check.checkName(list) || !Check.checkDistance(list)){
				System.out.println("出现错误");
				WriteListToCSV.writeCSVFromList(Check.list, city + lclass + "error.txt");
			}
			else {
				GetConstruct.getConTable(list);
				System.out.println("结构化表格书写...");
				WriteListToCSV.writeCSVFromList(GetConstruct.list, filepathString);
			}
		}else if (flag == 1) {
			GetConstruct.getConTable(list);
			System.out.println("结构化表格书写...");
			WriteListToCSV.writeCSVFromList(GetConstruct.list, filepathString);
		}else {
			System.out.println("flag只能输入0或者1,0表示检查错误,1表示不检查错误");
		}
	}
	
	public static void CarryOut2(String city, String lclass, int flag){
		String filepathString = city + lclass + "cons.tsv";
		extractData2(city, lclass);
		System.out.println("表格书写...");
		WriteListToCSV.writeCSVFromList(list, city + lclass + ".tsv");
		
		if(flag == 0){
			if(!Check.checkName(list) || !Check.checkDistance(list)){
				System.out.println("出现错误");
				WriteListToCSV.writeCSVFromList(Check.list, city + lclass + "error.txt");
			}
			else {
				GetConstruct.getConTable2(list);
				System.out.println("结构化表格书写...");
				WriteListToCSV.writeCSVFromList(GetConstruct.list, filepathString);
			}
		}else if (flag == 1) {
			GetConstruct.getConTable2(list);
			System.out.println("结构化表格书写...");
			WriteListToCSV.writeCSVFromList(GetConstruct.list, filepathString);
		}else {
			System.out.println("flag只能输入0或者1,0表示检查错误,1表示不检查错误");
		}
	}
	
	/**
	 * 存储一次查询的结果
	 */
	public static List<String> list = new ArrayList<String>();
	
	static{
		list.add("名称\t\tDATAID\t大类\t小类\tX\tY\t城市名称\tKeyword");
	}
	
	/**
	 * 从数据库中获取数据(主点为5A4A景点，知名大学，三级医院)
	 * @param city 查询SQL语言中查询的城市
	 * @param keyClass 查询SQL语言中查询的主点
	 * @param subClass 查询SQL语言中查询的副点
	 */
	public static void extractData(String city, String keyClass){
		SqlDo sqlDo = null;
		Connection connection = null;
		PreparedStatement pStatement = null;
		ResultSet reSet = null;
		try {
			sqlDo = new SqlDo(database.DbConst.DbType.SPACE_DB);
			connection = sqlDo.getConn();
			String sql1 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' and 所属城市 = \'"
                   + city + "\' and 小类 = \'" + keyClass + "\'";
			pStatement = connection.prepareStatement(sql1);
			reSet = pStatement.executeQuery();
			while (reSet.next()) {
				String keyName = reSet.getString(1);
				String sql2 = null;
				boolean isAdd = false;
				
				//大门
				sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
						"and 所属城市 = \'" + city + "\' and 小类= \'大门\'" +
						"and name like \'%" + keyName + "%\'";
				PreparedStatement pStatement2 = connection.prepareStatement(sql2);
				ResultSet reSet2 = pStatement2.executeQuery();
				
 				isAdd = HandleResultset(reSet, reSet2, isAdd);
 				pStatement2.close();
				reSet2.close();
				
				//停车场
				sql2 = "select * from (select * from T_POI t where 所属城市 = \'" + city + "\' and" +
						" 小类 = \'停车场\' and keyword like \'%NEWMANUAL%\' and name like \'%" + keyName + "%\' ) where " +
						"(keyword like \'%地下%\' and keyword like \'%地上%\' and keyword not like \'%内部%\' and keyword not " +
						"like \'%员工%\' and keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not like \'%内部%\' " +
						"and name not like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\') or " +
						"(keyword not like \'%地下%\' and keyword not like \'%地上%\' and keyword not like \'%内部%\' and keyword " +
						"not like \'%员工%\' and keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not " +
						"like \'%内部%\' and name not like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\')";
				pStatement2 = connection.prepareStatement(sql2);
				reSet2 = pStatement2.executeQuery();
				
				isAdd = HandleResultset(reSet, reSet2, isAdd);
				pStatement2.close();
				reSet2.close();

				//地下停车场
				sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
						"and 所属城市 = \'" + city + "\' and 小类 = \'停车场\' and keyword like \'%NEWMANUAL%%地下%\')" +
						" where keyword not like '%地上%' and keyword not like \'%内部%\' and keyword not like \'%员工%\' and " +
						"keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not like \'%内部%\' and name " +
						"not like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\'";
				pStatement2 = connection.prepareStatement(sql2);
				reSet2 = pStatement2.executeQuery();
				
				isAdd = HandleResultset(reSet, reSet2, isAdd);
				pStatement2.close();
				reSet2.close();
				
				//地上停车场
				sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
						"and 所属城市 = \'" + city + "\' and 小类 = \'停车场\' and keyword like \'%NEWMANUAL%%地上%\')" +
								" where keyword not like '%地下%' and keyword not like \'%内部%\' and keyword not like \'%员工%\' and" +
								" keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not like \'%内部%\' and name not " +
								"like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\'";
				pStatement2 = connection.prepareStatement(sql2);
				reSet2 = pStatement2.executeQuery();
				
				isAdd = HandleResultset(reSet, reSet2, isAdd);
				pStatement2.close();
				reSet2.close();
				
				if (keyClass.equals("5A4A景点")) {
					//售票处
					sql2 = "select * from (select * from T_POI t where 所属城市 = \'" + city + "\'" +
							"and keyword like \'%NEWMANUAL%\' and name like \'%" + keyName + "%\') where keyword like \'%售票处%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
				}
				
//				pStatement2.close();
//				reSet2.close();
			}
		} catch (Exception e) {
			System.out.println("QUERY FAILED");
			e.printStackTrace();
		}finally{
			if(connection!=null)
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			System.out.println("已存入List");
			sqlDo.closeRsAndPs(reSet, pStatement);
			sqlDo.freeConn();
		}
	}

	/**
	 * 从数据库中获取数据(主点为机场，航站楼)
	 * @param city 查询SQL语言中查询的城市
	 * @param keyClass 查询SQL语言中查询的主点
	 * @param subClass 查询SQL语言中查询的副点
	 */
	public static void extractData2(String city, String keyClass) {
		SqlDo sqlDo = null;
		Connection connection = null;
		PreparedStatement pStatement = null;
		ResultSet reSet = null;
		try {
			sqlDo = new SqlDo(database.DbConst.DbType.SPACE_DB);
			connection = sqlDo.getConn();
			String sql1 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' and 所属城市 = \'"
                   + city + "\' and 小类 = \'" + keyClass + "\'";
			pStatement = connection.prepareStatement(sql1);
			reSet = pStatement.executeQuery();
			while (reSet.next()) {
				String keyName = reSet.getString(1);
				String sql2 = null;
				boolean isAdd = false;
				PreparedStatement pStatement2 = null;
				ResultSet reSet2 = null;
				
				if (keyClass.equals("机场")) {
					//航站楼
					sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
							"and 所属城市 = \'" + city + "\' and 小类= \'航站楼\'" +
							"and name like \'%" + keyName + "%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
	 				isAdd = HandleResultset2(reSet, reSet2, isAdd);
	 				
	 				pStatement2.close();
					reSet2.close();
				} else if (keyClass.equals("航站楼")) {
					//出发
					sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
							"and 所属城市 = \'" + city + "\' and 大类= \'交通出行\' and name like \'%出发\'" +
							"and name like \'%" + keyName + "%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
	 				isAdd = HandleResultset2(reSet, reSet2, isAdd);
	 				
	 				pStatement2.close();
					reSet2.close();
					
					//到达
					sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
							"and 所属城市 = \'" + city + "\' and 大类= \'交通出行\' and name like \'%到达\'" +
							"and name like \'%" + keyName + "%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
	 				isAdd = HandleResultset2(reSet, reSet2, isAdd);
	 				
	 				pStatement2.close();
					reSet2.close();
					
					//落客区
					sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
							"and 所属城市 = \'" + city + "\' and 小类= \'落客区\'" +
							"and name like \'%" + keyName + "%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
	 				isAdd = HandleResultset2(reSet, reSet2, isAdd);
	 				
	 				pStatement2.close();
					reSet2.close();
					
					//停车场
					sql2 = "select * from (select * from T_POI t where 所属城市 = \'" + city + "\' and" +
							" 小类 = \'停车场\' and keyword like \'%NEWMANUAL%\' and name like \'%" + keyName + "%\' ) where " +
							"(keyword like \'%地下%\' and keyword like \'%地上%\' and keyword not like \'%内部%\' and keyword not " +
							"like \'%员工%\' and keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not like \'%内部%\' " +
							"and name not like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\') or " +
							"(keyword not like \'%地下%\' and keyword not like \'%地上%\' and keyword not like \'%内部%\' and keyword " +
							"not like \'%员工%\' and keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not " +
							"like \'%内部%\' and name not like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\')";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();

					//地下停车场
					sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
							"and 所属城市 = \'" + city + "\' and 小类 = \'停车场\' and keyword like \'%NEWMANUAL%%地下%\')" +
							" where keyword not like '%地上%' and keyword not like \'%内部%\' and keyword not like \'%员工%\' and " +
							"keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not like \'%内部%\' and name " +
							"not like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
					//地上停车场
					sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
							"and 所属城市 = \'" + city + "\' and 小类 = \'停车场\' and keyword like \'%NEWMANUAL%%地上%\')" +
									" where keyword not like '%地下%' and keyword not like \'%内部%\' and keyword not like \'%员工%\' and" +
									" keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not like \'%内部%\' and name not " +
									"like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
				} else if (keyClass.equals("大型火车站")) {
					//进站口
					sql2 = "select * from T_POI t where 所属城市=\'" + city + "\' and (name like \'%进口%\' " +
							"or name like \'%进站口%\') and name like \'%" + keyName + "%\' and keyword like \'%NEWMANUAL%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
					//出站口
					sql2 = "select * from T_POI t where 所属城市=\'" + city + "\' and (name like \'%出口%\' " +
							"or name like \'%出站口%\') and name like \'%" + keyName + "%\' and keyword like \'%NEWMANUAL%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
					//进出站口
					sql2 = "select * from T_POI t where 所属城市=\'" + city + "\' and (name like \'%出入口%\' " +
							"or name like \'%进出站口%\') and name like \'%" + keyName + "%\' and keyword like \'%NEWMANUAL%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
					//落客区
					sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
							"and 所属城市 = \'" + city + "\' and 小类= \'落客区\'" +
							"and name like \'%" + keyName + "%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
	 				isAdd = HandleResultset2(reSet, reSet2, isAdd);
	 				
	 				pStatement2.close();
					reSet2.close();
					
					//停车场
					sql2 = "select * from (select * from T_POI t where 所属城市 = \'" + city + "\' and" +
							" 小类 = \'停车场\' and keyword like \'%NEWMANUAL%\' and name like \'%" + keyName + "%\' ) where " +
							"(keyword like \'%地下%\' and keyword like \'%地上%\' and keyword not like \'%内部%\' and keyword not " +
							"like \'%员工%\' and keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not like \'%内部%\' " +
							"and name not like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\') or " +
							"(keyword not like \'%地下%\' and keyword not like \'%地上%\' and keyword not like \'%内部%\' and keyword " +
							"not like \'%员工%\' and keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not " +
							"like \'%内部%\' and name not like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\')";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();

					//地下停车场
					sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
							"and 所属城市 = \'" + city + "\' and 小类 = \'停车场\' and keyword like \'%NEWMANUAL%%地下%\')" +
							" where keyword not like '%地上%' and keyword not like \'%内部%\' and keyword not like \'%员工%\' and " +
							"keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not like \'%内部%\' and name " +
							"not like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
					//地上停车场
					sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
							"and 所属城市 = \'" + city + "\' and 小类 = \'停车场\' and keyword like \'%NEWMANUAL%%地上%\')" +
									" where keyword not like '%地下%' and keyword not like \'%内部%\' and keyword not like \'%员工%\' and" +
									" keyword not like \'%出租车%\' and keyword not like \'%大客车%\' and name not like \'%内部%\' and name not " +
									"like \'%员工%\' and name not like \'%出租车%\' and name not like \'%大客车%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
				}
				
			}
		} catch (Exception e) {
			System.out.println("QUERY FAILED");
			e.printStackTrace();
		}finally{
			if(connection!=null)
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			System.out.println("已存入List");
			sqlDo.closeRsAndPs(reSet, pStatement);
			sqlDo.freeConn();
		}
	}
	
	public static boolean HandleResultset(ResultSet reSet, ResultSet reSet2, boolean isAdd) throws SQLException{
		DecimalFormat decimalFormat = new DecimalFormat("########.########");
		JGeometry jg1 = null;
		STRUCT o1 = (STRUCT)reSet.getObject("GEOLOC");
		if (o1 != null) {
			jg1 = JGeometry.load(o1);
		}
		double[] point1 = jg1.getPoint();
		if (reSet2.next()) {
			if (!isAdd) {
				list.add(reSet.getString(1) + "\t\t" + "1_" + reSet.getString(8) + "\t" + reSet.getString(14) + "\t" + reSet.getString(15) + "\t" + decimalFormat.format(point1[0]) + "\t" + decimalFormat.format(point1[1]) + "\t" + reSet.getString(35) + "\t" + reSet.getString(5));
				isAdd = true;
			}
			String[] strings1 = reSet2.getString(1).split("-");
			JGeometry jg = null;
			STRUCT o2 = (STRUCT)reSet2.getObject("GEOLOC");
			if (o2 != null) {
				jg = JGeometry.load(o2);
			}
			double[] point = jg.getPoint();
			if (strings1[0].equals(reSet.getString(1))) {
				list.add(reSet2.getString(1) + "\t" + strings1[1] + "\t" + reSet2.getString(8) + "\t" + reSet2.getString(14) + "\t" + reSet2.getString(15) + "\t" + decimalFormat.format(point[0]) + "\t" + decimalFormat.format(point[1]) + "\t" + reSet2.getString(35) + "\t" + reSet2.getString(5));
			}
			while (reSet2.next()) {
				String[] strings2 = reSet2.getString(1).split("-");
				JGeometry jg2 = null;
				STRUCT o22 = (STRUCT)reSet2.getObject("GEOLOC");
				if (o2 != null) {
					jg2 = JGeometry.load(o22);
				}
				double[] point2 = jg2.getPoint();
				if (strings2[0].equals(reSet.getString(1))) {
					list.add(reSet2.getString(1) + "\t" + strings2[1] + "\t" + reSet2.getString(8) + "\t" + reSet2.getString(14) + "\t" + reSet2.getString(15) + "\t" + decimalFormat.format(point2[0]) + "\t" + decimalFormat.format(point2[1]) + "\t" + reSet2.getString(35) + "\t" + reSet2.getString(5));
				}
			}
		}
		return isAdd;
	}
	
	public static boolean HandleResultset2(ResultSet reSet, ResultSet reSet2, boolean isAdd) throws SQLException{
		DecimalFormat decimalFormat = new DecimalFormat("########.########");
		JGeometry jg1 = null;
		STRUCT o1 = (STRUCT)reSet.getObject("GEOLOC");
		if (o1 != null) {
			jg1 = JGeometry.load(o1);
		}
		double[] point1 = jg1.getPoint();
		if (reSet2.next()) {
			if (!isAdd) {
				list.add(reSet.getString(1) + "\t\t" + "1_" + reSet.getString(8) + "\t" + reSet.getString(14) + "\t" + reSet.getString(15) + "\t" + decimalFormat.format(point1[0]) + "\t" + decimalFormat.format(point1[1]) + "\t" + reSet.getString(35) + "\t" + reSet.getString(5));
				isAdd = true;
			}
			String string = reSet2.getString(1).substring(reSet.getString(1).length(), reSet2.getString(1).length());
			JGeometry jg = null;
			STRUCT o2 = (STRUCT)reSet2.getObject("GEOLOC");
			if (o2 != null) {
				jg = JGeometry.load(o2);
			}
			double[] point = jg.getPoint();
			list.add(reSet2.getString(1) + "\t" + string + "\t" + reSet2.getString(8) + "\t" + reSet2.getString(14) + "\t" + reSet2.getString(15) + "\t" + decimalFormat.format(point[0]) + "\t" + decimalFormat.format(point[1]) + "\t" + reSet2.getString(35) + "\t" + reSet2.getString(5));
			while (reSet2.next()) {
				String string2 = reSet2.getString(1).substring(reSet.getString(1).length(), reSet2.getString(1).length());
				JGeometry jg2 = null;
				STRUCT o22 = (STRUCT)reSet2.getObject("GEOLOC");
				if (o2 != null) {
					jg2 = JGeometry.load(o22);
				}
				double[] point2 = jg2.getPoint();
				list.add(reSet2.getString(1) + "\t" + string2 + "\t" + reSet2.getString(8) + "\t" + reSet2.getString(14) + "\t" + reSet2.getString(15) + "\t" + decimalFormat.format(point2[0]) + "\t" + decimalFormat.format(point2[1]) + "\t" + reSet2.getString(35) + "\t" + reSet2.getString(5));
			}
		}
		return isAdd;
	}
	
}
