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
		String city = "������";
		String lclass = "֪����ѧ";
		CarryOut(city, lclass, 1);
//		CarryOut2(city, lclass, 1);
	}
	
	public static void CarryOut(String city, String lclass, int flag){
		String filepathString = city + lclass + "cons.tsv";
		extractData(city, lclass);
		System.out.println("�����д...");
		WriteListToCSV.writeCSVFromList(list, city + lclass + ".tsv");
		
		if(flag == 0){
			if(!Check.checkName(list) || !Check.checkDistance(list)){
				System.out.println("���ִ���");
				WriteListToCSV.writeCSVFromList(Check.list, city + lclass + "error.txt");
			}
			else {
				GetConstruct.getConTable(list);
				System.out.println("�ṹ�������д...");
				WriteListToCSV.writeCSVFromList(GetConstruct.list, filepathString);
			}
		}else if (flag == 1) {
			GetConstruct.getConTable(list);
			System.out.println("�ṹ�������д...");
			WriteListToCSV.writeCSVFromList(GetConstruct.list, filepathString);
		}else {
			System.out.println("flagֻ������0����1,0��ʾ������,1��ʾ��������");
		}
	}
	
	public static void CarryOut2(String city, String lclass, int flag){
		String filepathString = city + lclass + "cons.tsv";
		extractData2(city, lclass);
		System.out.println("�����д...");
		WriteListToCSV.writeCSVFromList(list, city + lclass + ".tsv");
		
		if(flag == 0){
			if(!Check.checkName(list) || !Check.checkDistance(list)){
				System.out.println("���ִ���");
				WriteListToCSV.writeCSVFromList(Check.list, city + lclass + "error.txt");
			}
			else {
				GetConstruct.getConTable2(list);
				System.out.println("�ṹ�������д...");
				WriteListToCSV.writeCSVFromList(GetConstruct.list, filepathString);
			}
		}else if (flag == 1) {
			GetConstruct.getConTable2(list);
			System.out.println("�ṹ�������д...");
			WriteListToCSV.writeCSVFromList(GetConstruct.list, filepathString);
		}else {
			System.out.println("flagֻ������0����1,0��ʾ������,1��ʾ��������");
		}
	}
	
	/**
	 * �洢һ�β�ѯ�Ľ��
	 */
	public static List<String> list = new ArrayList<String>();
	
	static{
		list.add("����\t\tDATAID\t����\tС��\tX\tY\t��������\tKeyword");
	}
	
	/**
	 * �����ݿ��л�ȡ����(����Ϊ5A4A���㣬֪����ѧ������ҽԺ)
	 * @param city ��ѯSQL�����в�ѯ�ĳ���
	 * @param keyClass ��ѯSQL�����в�ѯ������
	 * @param subClass ��ѯSQL�����в�ѯ�ĸ���
	 */
	public static void extractData(String city, String keyClass){
		SqlDo sqlDo = null;
		Connection connection = null;
		PreparedStatement pStatement = null;
		ResultSet reSet = null;
		try {
			sqlDo = new SqlDo(database.DbConst.DbType.SPACE_DB);
			connection = sqlDo.getConn();
			String sql1 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' and �������� = \'"
                   + city + "\' and С�� = \'" + keyClass + "\'";
			pStatement = connection.prepareStatement(sql1);
			reSet = pStatement.executeQuery();
			while (reSet.next()) {
				String keyName = reSet.getString(1);
				String sql2 = null;
				boolean isAdd = false;
				
				//����
				sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
						"and �������� = \'" + city + "\' and С��= \'����\'" +
						"and name like \'%" + keyName + "%\'";
				PreparedStatement pStatement2 = connection.prepareStatement(sql2);
				ResultSet reSet2 = pStatement2.executeQuery();
				
 				isAdd = HandleResultset(reSet, reSet2, isAdd);
 				pStatement2.close();
				reSet2.close();
				
				//ͣ����
				sql2 = "select * from (select * from T_POI t where �������� = \'" + city + "\' and" +
						" С�� = \'ͣ����\' and keyword like \'%NEWMANUAL%\' and name like \'%" + keyName + "%\' ) where " +
						"(keyword like \'%����%\' and keyword like \'%����%\' and keyword not like \'%�ڲ�%\' and keyword not " +
						"like \'%Ա��%\' and keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not like \'%�ڲ�%\' " +
						"and name not like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\') or " +
						"(keyword not like \'%����%\' and keyword not like \'%����%\' and keyword not like \'%�ڲ�%\' and keyword " +
						"not like \'%Ա��%\' and keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not " +
						"like \'%�ڲ�%\' and name not like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\')";
				pStatement2 = connection.prepareStatement(sql2);
				reSet2 = pStatement2.executeQuery();
				
				isAdd = HandleResultset(reSet, reSet2, isAdd);
				pStatement2.close();
				reSet2.close();

				//����ͣ����
				sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
						"and �������� = \'" + city + "\' and С�� = \'ͣ����\' and keyword like \'%NEWMANUAL%%����%\')" +
						" where keyword not like '%����%' and keyword not like \'%�ڲ�%\' and keyword not like \'%Ա��%\' and " +
						"keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not like \'%�ڲ�%\' and name " +
						"not like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\'";
				pStatement2 = connection.prepareStatement(sql2);
				reSet2 = pStatement2.executeQuery();
				
				isAdd = HandleResultset(reSet, reSet2, isAdd);
				pStatement2.close();
				reSet2.close();
				
				//����ͣ����
				sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
						"and �������� = \'" + city + "\' and С�� = \'ͣ����\' and keyword like \'%NEWMANUAL%%����%\')" +
								" where keyword not like '%����%' and keyword not like \'%�ڲ�%\' and keyword not like \'%Ա��%\' and" +
								" keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not like \'%�ڲ�%\' and name not " +
								"like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\'";
				pStatement2 = connection.prepareStatement(sql2);
				reSet2 = pStatement2.executeQuery();
				
				isAdd = HandleResultset(reSet, reSet2, isAdd);
				pStatement2.close();
				reSet2.close();
				
				if (keyClass.equals("5A4A����")) {
					//��Ʊ��
					sql2 = "select * from (select * from T_POI t where �������� = \'" + city + "\'" +
							"and keyword like \'%NEWMANUAL%\' and name like \'%" + keyName + "%\') where keyword like \'%��Ʊ��%\'";
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
			System.out.println("�Ѵ���List");
			sqlDo.closeRsAndPs(reSet, pStatement);
			sqlDo.freeConn();
		}
	}

	/**
	 * �����ݿ��л�ȡ����(����Ϊ��������վ¥)
	 * @param city ��ѯSQL�����в�ѯ�ĳ���
	 * @param keyClass ��ѯSQL�����в�ѯ������
	 * @param subClass ��ѯSQL�����в�ѯ�ĸ���
	 */
	public static void extractData2(String city, String keyClass) {
		SqlDo sqlDo = null;
		Connection connection = null;
		PreparedStatement pStatement = null;
		ResultSet reSet = null;
		try {
			sqlDo = new SqlDo(database.DbConst.DbType.SPACE_DB);
			connection = sqlDo.getConn();
			String sql1 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' and �������� = \'"
                   + city + "\' and С�� = \'" + keyClass + "\'";
			pStatement = connection.prepareStatement(sql1);
			reSet = pStatement.executeQuery();
			while (reSet.next()) {
				String keyName = reSet.getString(1);
				String sql2 = null;
				boolean isAdd = false;
				PreparedStatement pStatement2 = null;
				ResultSet reSet2 = null;
				
				if (keyClass.equals("����")) {
					//��վ¥
					sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
							"and �������� = \'" + city + "\' and С��= \'��վ¥\'" +
							"and name like \'%" + keyName + "%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
	 				isAdd = HandleResultset2(reSet, reSet2, isAdd);
	 				
	 				pStatement2.close();
					reSet2.close();
				} else if (keyClass.equals("��վ¥")) {
					//����
					sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
							"and �������� = \'" + city + "\' and ����= \'��ͨ����\' and name like \'%����\'" +
							"and name like \'%" + keyName + "%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
	 				isAdd = HandleResultset2(reSet, reSet2, isAdd);
	 				
	 				pStatement2.close();
					reSet2.close();
					
					//����
					sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
							"and �������� = \'" + city + "\' and ����= \'��ͨ����\' and name like \'%����\'" +
							"and name like \'%" + keyName + "%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
	 				isAdd = HandleResultset2(reSet, reSet2, isAdd);
	 				
	 				pStatement2.close();
					reSet2.close();
					
					//�����
					sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
							"and �������� = \'" + city + "\' and С��= \'�����\'" +
							"and name like \'%" + keyName + "%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
	 				isAdd = HandleResultset2(reSet, reSet2, isAdd);
	 				
	 				pStatement2.close();
					reSet2.close();
					
					//ͣ����
					sql2 = "select * from (select * from T_POI t where �������� = \'" + city + "\' and" +
							" С�� = \'ͣ����\' and keyword like \'%NEWMANUAL%\' and name like \'%" + keyName + "%\' ) where " +
							"(keyword like \'%����%\' and keyword like \'%����%\' and keyword not like \'%�ڲ�%\' and keyword not " +
							"like \'%Ա��%\' and keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not like \'%�ڲ�%\' " +
							"and name not like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\') or " +
							"(keyword not like \'%����%\' and keyword not like \'%����%\' and keyword not like \'%�ڲ�%\' and keyword " +
							"not like \'%Ա��%\' and keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not " +
							"like \'%�ڲ�%\' and name not like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\')";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();

					//����ͣ����
					sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
							"and �������� = \'" + city + "\' and С�� = \'ͣ����\' and keyword like \'%NEWMANUAL%%����%\')" +
							" where keyword not like '%����%' and keyword not like \'%�ڲ�%\' and keyword not like \'%Ա��%\' and " +
							"keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not like \'%�ڲ�%\' and name " +
							"not like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
					//����ͣ����
					sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
							"and �������� = \'" + city + "\' and С�� = \'ͣ����\' and keyword like \'%NEWMANUAL%%����%\')" +
									" where keyword not like '%����%' and keyword not like \'%�ڲ�%\' and keyword not like \'%Ա��%\' and" +
									" keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not like \'%�ڲ�%\' and name not " +
									"like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
				} else if (keyClass.equals("���ͻ�վ")) {
					//��վ��
					sql2 = "select * from T_POI t where ��������=\'" + city + "\' and (name like \'%����%\' " +
							"or name like \'%��վ��%\') and name like \'%" + keyName + "%\' and keyword like \'%NEWMANUAL%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
					//��վ��
					sql2 = "select * from T_POI t where ��������=\'" + city + "\' and (name like \'%����%\' " +
							"or name like \'%��վ��%\') and name like \'%" + keyName + "%\' and keyword like \'%NEWMANUAL%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
					//����վ��
					sql2 = "select * from T_POI t where ��������=\'" + city + "\' and (name like \'%�����%\' " +
							"or name like \'%����վ��%\') and name like \'%" + keyName + "%\' and keyword like \'%NEWMANUAL%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
					//�����
					sql2 = "select * from T_POI t where keyword like \'%NEWMANUAL%\' " +
							"and �������� = \'" + city + "\' and С��= \'�����\'" +
							"and name like \'%" + keyName + "%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
	 				isAdd = HandleResultset2(reSet, reSet2, isAdd);
	 				
	 				pStatement2.close();
					reSet2.close();
					
					//ͣ����
					sql2 = "select * from (select * from T_POI t where �������� = \'" + city + "\' and" +
							" С�� = \'ͣ����\' and keyword like \'%NEWMANUAL%\' and name like \'%" + keyName + "%\' ) where " +
							"(keyword like \'%����%\' and keyword like \'%����%\' and keyword not like \'%�ڲ�%\' and keyword not " +
							"like \'%Ա��%\' and keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not like \'%�ڲ�%\' " +
							"and name not like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\') or " +
							"(keyword not like \'%����%\' and keyword not like \'%����%\' and keyword not like \'%�ڲ�%\' and keyword " +
							"not like \'%Ա��%\' and keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not " +
							"like \'%�ڲ�%\' and name not like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\')";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();

					//����ͣ����
					sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
							"and �������� = \'" + city + "\' and С�� = \'ͣ����\' and keyword like \'%NEWMANUAL%%����%\')" +
							" where keyword not like '%����%' and keyword not like \'%�ڲ�%\' and keyword not like \'%Ա��%\' and " +
							"keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not like \'%�ڲ�%\' and name " +
							"not like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\'";
					pStatement2 = connection.prepareStatement(sql2);
					reSet2 = pStatement2.executeQuery();
					
					isAdd = HandleResultset2(reSet, reSet2, isAdd);
					pStatement2.close();
					reSet2.close();
					
					//����ͣ����
					sql2 = "select * from (select * from T_POI t where name like \'%" + keyName + "%\' " +
							"and �������� = \'" + city + "\' and С�� = \'ͣ����\' and keyword like \'%NEWMANUAL%%����%\')" +
									" where keyword not like '%����%' and keyword not like \'%�ڲ�%\' and keyword not like \'%Ա��%\' and" +
									" keyword not like \'%���⳵%\' and keyword not like \'%��ͳ�%\' and name not like \'%�ڲ�%\' and name not " +
									"like \'%Ա��%\' and name not like \'%���⳵%\' and name not like \'%��ͳ�%\'";
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
			System.out.println("�Ѵ���List");
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
