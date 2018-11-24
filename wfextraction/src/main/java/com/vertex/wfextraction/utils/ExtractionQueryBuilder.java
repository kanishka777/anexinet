package com.vertex.wfextraction.utils;

import java.util.List;

import com.vertex.wfextraction.domain.Column;
import com.vertex.wfextraction.domain.ColumnHelper;

public class ExtractionQueryBuilder {

	public static String getQuery(ColumnHelper helper, String version) {

		StringBuilder sql = new StringBuilder();
		List<Column> columns = helper.getColumnList();
		sql.append("SELECT\n");
		int index = 0;
		for (Column col : columns) {

			index++;
			String queryCol = col.getQueryCol(version);
			if ((queryCol == null) || (!col.versionApplies(version))) {
				System.out.println("'" + col.getName() + ":" + col.getType().name() + "'");
				sql.append("null");
			} else if (queryCol.contains(".")) {
				sql.append(queryCol);
			} else {
				sql.append(queryCol + "." + col.getName());
			}

			sql.append(" AS " + col.getName() + ((index < columns.size()) ? "," : "") + "\n");
		}
		sql.append("FROM RDBLineItem LI\n");
		sql.append("INNER JOIN RDBLineItemRef REF ON REF.lineItemId=LI.lineItemId\n");
		sql.append("INNER JOIN RDBSource SS ON SS.sourceId=REF.sourceId\n");
		sql.append("INNER JOIN RDBTransactionType TRANSTYPE ON TRANSTYPE.transactionTypeId=LI.transactionTypeId\n");
		sql.append(
				"INNER JOIN RDBTransactionStatusType TRANSSTATUS ON TRANSSTATUS.transStatusTypeId=REF.transStatusTypeId AND TRANSSTATUS.transStatusTypeName='Active'\n");
		sql.append("LEFT OUTER JOIN RDBPartyRoleType TRANSPER ON TRANSPER.partyRoleTypeId=LI.transPrspctvTypeId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDim ITEMTYPE ON LI.itemTypeRDBId!=-1 AND ITEMTYPE.txbltyDvrRDBId=LI.itemTypeRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDetail ITEMTYPEN ON ITEMTYPEN.txbltyDvrDtlId=ITEMTYPE.txbltyDvrDtlId AND ITEMTYPEN.txbltyDvrSrcId=ITEMTYPE.txbltyDvrSrcId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDim ITEMCLASS ON LI.itemClassRDBId!=-1 AND ITEMCLASS.txbltyDvrRDBId=LI.itemClassRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDetail ITEMCLASSN ON ITEMCLASSN.txbltyDvrDtlId=ITEMCLASS.txbltyDvrDtlId AND ITEMCLASSN.txbltyDvrSrcId=ITEMCLASS.txbltyDvrSrcId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDim USAGETYPE ON LI.usageTypeRDBId!=-1 AND USAGETYPE.txbltyDvrRDBId=LI.usageTypeRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDetail USAGETYPEN ON USAGETYPEN.txbltyDvrDtlId=USAGETYPE.txbltyDvrDtlId AND USAGETYPEN.txbltyDvrSrcId=USAGETYPE.txbltyDvrSrcId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDim USAGECLASS ON LI.usageClassRDBId!=-1 AND USAGECLASS.txbltyDvrRDBId=LI.usageClassRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDetail USAGECLASSN ON USAGECLASSN.txbltyDvrDtlId=USAGECLASS.txbltyDvrDtlId AND USAGECLASSN.txbltyDvrSrcId=USAGECLASS.txbltyDvrSrcId\n");

		sql.append("LEFT OUTER JOIN RDBSimplificationType SIMP ON SIMP.simpTypeId=LI.simpTypeId\n");
		sql.append("INNER JOIN RDBBasisType BASISTYPE ON BASISTYPE.basisTypeId=LI.basisTypeId\n");
		sql.append(
				"LEFT OUTER JOIN RDBShippingTerms SHIPTERM ON LI.shippingTermsId!=-1 AND SHIPTERM.shippingTermsId=LI.shippingTermsId\n");
		sql.append("LEFT OUTER JOIN RDBChainTransType CHAIN ON CHAIN.chainTransId=LI.chainTransId\n");
		sql.append("LEFT OUTER JOIN RDBTitleTrnsfrType TITLETRANS ON TITLETRANS.titleTransferId=LI.titleTransferId\n");
		sql.append("INNER JOIN RDBBusTransType BUSTRANS ON BUSTRANS.busTransTypeId=LI.busTransTypeId\n");
		sql.append("INNER JOIN RDBCurrencyUnit CURUNIT ON CURUNIT.currencyUnitId=LI.currencyUnitId\n");
		sql.append("INNER JOIN RDBCurrencyUnit ORIGCURUNIT ON ORIGCURUNIT.currencyUnitId=LI.origCurrencyUnitId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDim COSTCENTER ON LI.costCenterRDBId!=-1 AND COSTCENTER.txbltyDvrRDBId=LI.costCenterRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDetail COSTCENTERN ON COSTCENTERN.txbltyDvrDtlId=COSTCENTER.txbltyDvrDtlId AND COSTCENTERN.txbltyDvrSrcId=COSTCENTER.txbltyDvrSrcId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDim DEPARTMENT ON LI.departmentRDBId!=-1 AND DEPARTMENT.txbltyDvrRDBId=LI.departmentRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDetail DEPARTMENTN ON DEPARTMENTN.txbltyDvrDtlId=DEPARTMENT.txbltyDvrDtlId AND DEPARTMENTN.txbltyDvrSrcId=DEPARTMENT.txbltyDvrSrcId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDim GLACCT ON LI.genLdgrAcctRDBId!=-1 AND GLACCT.txbltyDvrRDBId=LI.genLdgrAcctRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDetail GLACCTN ON GLACCTN.txbltyDvrDtlId=GLACCT.txbltyDvrDtlId AND GLACCTN.txbltyDvrSrcId=GLACCT.txbltyDvrSrcId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDim MATCODE ON LI.materialCodeRDBId!=-1 AND MATCODE.txbltyDvrRDBId=LI.materialCodeRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDetail MATCODEN ON MATCODEN.txbltyDvrDtlId=MATCODE.txbltyDvrDtlId AND MATCODEN.txbltyDvrSrcId=MATCODE.txbltyDvrSrcId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDim PROJECT ON LI.projectRDBId!=-1 AND PROJECT.txbltyDvrRDBId=LI.projectRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDetail PROJECTN ON PROJECTN.txbltyDvrDtlId=PROJECT.txbltyDvrDtlId AND PROJECTN.txbltyDvrSrcId=PROJECT.txbltyDvrSrcId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDim SKU ON LI.vendorSkuRDBId!=-1 AND SKU.txbltyDvrRDBId=LI.vendorSkuRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDetail SKUN ON SKUN.txbltyDvrDtlId=SKU.txbltyDvrDtlId AND SKUN.txbltyDvrSrcId=SKU.txbltyDvrSrcId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDim COMCODE ON LI.commodityCodeRDBId!=-1 AND COMCODE.txbltyDvrRDBId=LI.commodityCodeRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTxbltyDvrDetail COMCODEN ON COMCODEN.txbltyDvrDtlId=COMCODE.txbltyDvrDtlId AND COMCODEN.txbltyDvrSrcId=COMCODE.txbltyDvrSrcId\n");
		sql.append(
				"LEFT OUTER JOIN RDBInputParamType COMCODET ON COMCODET.inputParamTypeId=COMCODEN.inputParamTypeId AND COMCODET.commodityCodeInd=1\n");

		if (version.equals("7.0")) {

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX1 ON STRFLX1.lineItemId=LI.lineItemId AND STRFLX1.sourceId=LI.sourceId AND STRFLX1.flexFieldDefRefNum=1\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN1 ON STRFLXN1.flexFieldDefDtlId=STRFLX1.flexFieldDefDtlId AND STRFLXN1.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX2 ON STRFLX2.lineItemId=LI.lineItemId AND STRFLX2.sourceId=LI.sourceId AND STRFLX2.flexFieldDefRefNum=2\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN2 ON STRFLXN2.flexFieldDefDtlId=STRFLX2.flexFieldDefDtlId AND STRFLXN2.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX3 ON STRFLX3.lineItemId=LI.lineItemId AND STRFLX3.sourceId=LI.sourceId AND STRFLX3.flexFieldDefRefNum=3\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN3 ON STRFLXN3.flexFieldDefDtlId=STRFLX3.flexFieldDefDtlId AND STRFLXN3.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX4 ON STRFLX4.lineItemId=LI.lineItemId AND STRFLX4.sourceId=LI.sourceId AND STRFLX4.flexFieldDefRefNum=4\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN4 ON STRFLXN4.flexFieldDefDtlId=STRFLX4.flexFieldDefDtlId AND STRFLXN4.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX5 ON STRFLX5.lineItemId=LI.lineItemId AND STRFLX5.sourceId=LI.sourceId AND STRFLX5.flexFieldDefRefNum=5\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN5 ON STRFLXN5.flexFieldDefDtlId=STRFLX5.flexFieldDefDtlId AND STRFLXN5.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX6 ON STRFLX6.lineItemId=LI.lineItemId AND STRFLX6.sourceId=LI.sourceId AND STRFLX6.flexFieldDefRefNum=6\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN6 ON STRFLXN6.flexFieldDefDtlId=STRFLX6.flexFieldDefDtlId AND STRFLXN6.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX7 ON STRFLX7.lineItemId=LI.lineItemId AND STRFLX7.sourceId=LI.sourceId AND STRFLX7.flexFieldDefRefNum=7\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN7 ON STRFLXN7.flexFieldDefDtlId=STRFLX7.flexFieldDefDtlId AND STRFLXN7.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX8 ON STRFLX8.lineItemId=LI.lineItemId AND STRFLX8.sourceId=LI.sourceId AND STRFLX8.flexFieldDefRefNum=8\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN8 ON STRFLXN8.flexFieldDefDtlId=STRFLX8.flexFieldDefDtlId AND STRFLXN8.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX9 ON STRFLX9.lineItemId=LI.lineItemId AND STRFLX9.sourceId=LI.sourceId AND STRFLX9.flexFieldDefRefNum=9\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN9 ON STRFLXN9.flexFieldDefDtlId=STRFLX9.flexFieldDefDtlId AND STRFLXN9.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX10 ON STRFLX10.lineItemId=LI.lineItemId AND STRFLX10.sourceId=LI.sourceId AND STRFLX10.flexFieldDefRefNum=10\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN10 ON STRFLXN10.flexFieldDefDtlId=STRFLX10.flexFieldDefDtlId AND STRFLXN10.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX11 ON STRFLX11.lineItemId=LI.lineItemId AND STRFLX11.sourceId=LI.sourceId AND STRFLX11.flexFieldDefRefNum=11\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN11 ON STRFLXN11.flexFieldDefDtlId=STRFLX11.flexFieldDefDtlId AND STRFLXN11.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX12 ON STRFLX12.lineItemId=LI.lineItemId AND STRFLX12.sourceId=LI.sourceId AND STRFLX12.flexFieldDefRefNum=12\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN12 ON STRFLXN12.flexFieldDefDtlId=STRFLX12.flexFieldDefDtlId AND STRFLXN12.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX13 ON STRFLX13.lineItemId=LI.lineItemId AND STRFLX13.sourceId=LI.sourceId AND STRFLX13.flexFieldDefRefNum=13\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN13 ON STRFLXN13.flexFieldDefDtlId=STRFLX13.flexFieldDefDtlId AND STRFLXN13.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX14 ON STRFLX14.lineItemId=LI.lineItemId AND STRFLX14.sourceId=LI.sourceId AND STRFLX14.flexFieldDefRefNum=14\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN14 ON STRFLXN14.flexFieldDefDtlId=STRFLX14.flexFieldDefDtlId AND STRFLXN14.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX15 ON STRFLX15.lineItemId=LI.lineItemId AND STRFLX15.sourceId=LI.sourceId AND STRFLX15.flexFieldDefRefNum=15\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN15 ON STRFLXN15.flexFieldDefDtlId=STRFLX15.flexFieldDefDtlId AND STRFLXN15.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX16 ON STRFLX16.lineItemId=LI.lineItemId AND STRFLX16.sourceId=LI.sourceId AND STRFLX16.flexFieldDefRefNum=16\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN16 ON STRFLXN16.flexFieldDefDtlId=STRFLX16.flexFieldDefDtlId AND STRFLXN16.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX17 ON STRFLX17.lineItemId=LI.lineItemId AND STRFLX17.sourceId=LI.sourceId AND STRFLX17.flexFieldDefRefNum=17\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN17 ON STRFLXN17.flexFieldDefDtlId=STRFLX17.flexFieldDefDtlId AND STRFLXN17.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX18 ON STRFLX18.lineItemId=LI.lineItemId AND STRFLX18.sourceId=LI.sourceId AND STRFLX18.flexFieldDefRefNum=18\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN18 ON STRFLXN18.flexFieldDefDtlId=STRFLX18.flexFieldDefDtlId AND STRFLXN18.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX19 ON STRFLX19.lineItemId=LI.lineItemId AND STRFLX19.sourceId=LI.sourceId AND STRFLX19.flexFieldDefRefNum=19\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN19 ON STRFLXN19.flexFieldDefDtlId=STRFLX19.flexFieldDefDtlId AND STRFLXN19.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX20 ON STRFLX20.lineItemId=LI.lineItemId AND STRFLX20.sourceId=LI.sourceId AND STRFLX20.flexFieldDefRefNum=20\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN20 ON STRFLXN20.flexFieldDefDtlId=STRFLX20.flexFieldDefDtlId AND STRFLXN20.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX21 ON STRFLX21.lineItemId=LI.lineItemId AND STRFLX21.sourceId=LI.sourceId AND STRFLX21.flexFieldDefRefNum=21\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN21 ON STRFLXN21.flexFieldDefDtlId=STRFLX21.flexFieldDefDtlId AND STRFLXN21.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX22 ON STRFLX22.lineItemId=LI.lineItemId AND STRFLX22.sourceId=LI.sourceId AND STRFLX22.flexFieldDefRefNum=22\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN22 ON STRFLXN22.flexFieldDefDtlId=STRFLX22.flexFieldDefDtlId AND STRFLXN22.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX23 ON STRFLX23.lineItemId=LI.lineItemId AND STRFLX23.sourceId=LI.sourceId AND STRFLX23.flexFieldDefRefNum=23\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN23 ON STRFLXN23.flexFieldDefDtlId=STRFLX23.flexFieldDefDtlId AND STRFLXN23.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX24 ON STRFLX24.lineItemId=LI.lineItemId AND STRFLX24.sourceId=LI.sourceId AND STRFLX24.flexFieldDefRefNum=24\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN24 ON STRFLXN24.flexFieldDefDtlId=STRFLX24.flexFieldDefDtlId AND STRFLXN24.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX25 ON STRFLX25.lineItemId=LI.lineItemId AND STRFLX25.sourceId=LI.sourceId AND STRFLX25.flexFieldDefRefNum=25\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl STRFLXN25 ON STRFLXN25.flexFieldDefDtlId=STRFLX25.flexFieldDefDtlId AND STRFLXN25.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmDateFlxFld DATEFLX1 ON DATEFLX1.lineItemId=LI.lineItemId AND DATEFLX1.sourceId=LI.sourceId AND DATEFLX1.flexFieldDefRefNum=1\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl DATEFLXN1 ON DATEFLXN1.flexFieldDefDtlId=DATEFLX1.flexFieldDefDtlId AND DATEFLXN1.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmDateFlxFld DATEFLX2 ON DATEFLX2.lineItemId=LI.lineItemId AND DATEFLX2.sourceId=LI.sourceId AND DATEFLX2.flexFieldDefRefNum=2\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl DATEFLXN2 ON DATEFLXN2.flexFieldDefDtlId=DATEFLX2.flexFieldDefDtlId AND DATEFLXN2.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmDateFlxFld DATEFLX3 ON DATEFLX3.lineItemId=LI.lineItemId AND DATEFLX3.sourceId=LI.sourceId AND DATEFLX3.flexFieldDefRefNum=3\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl DATEFLXN3 ON DATEFLXN3.flexFieldDefDtlId=DATEFLX3.flexFieldDefDtlId AND DATEFLXN3.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmDateFlxFld DATEFLX4 ON DATEFLX4.lineItemId=LI.lineItemId AND DATEFLX4.sourceId=LI.sourceId AND DATEFLX4.flexFieldDefRefNum=4\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl DATEFLXN4 ON DATEFLXN4.flexFieldDefDtlId=DATEFLX4.flexFieldDefDtlId AND DATEFLXN4.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmDateFlxFld DATEFLX5 ON DATEFLX5.lineItemId=LI.lineItemId AND DATEFLX5.sourceId=LI.sourceId AND DATEFLX5.flexFieldDefRefNum=5\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl DATEFLXN5 ON DATEFLXN5.flexFieldDefDtlId=DATEFLX5.flexFieldDefDtlId AND DATEFLXN5.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmNumFlxFld NUMFLX1 ON NUMFLX1.lineItemId=LI.lineItemId AND NUMFLX1.sourceId=LI.sourceId AND NUMFLX1.flexFieldDefRefNum=1\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl NUMFLXN1 ON NUMFLXN1.flexFieldDefDtlId=NUMFLX1.flexFieldDefDtlId AND NUMFLXN1.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmNumFlxFld NUMFLX2 ON NUMFLX2.lineItemId=LI.lineItemId AND NUMFLX2.sourceId=LI.sourceId AND NUMFLX2.flexFieldDefRefNum=2\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl NUMFLXN2 ON NUMFLXN2.flexFieldDefDtlId=NUMFLX2.flexFieldDefDtlId AND NUMFLXN2.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmNumFlxFld NUMFLX3 ON NUMFLX3.lineItemId=LI.lineItemId AND NUMFLX3.sourceId=LI.sourceId AND NUMFLX3.flexFieldDefRefNum=3\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl NUMFLXN3 ON NUMFLXN3.flexFieldDefDtlId=NUMFLX3.flexFieldDefDtlId AND NUMFLXN3.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmNumFlxFld NUMFLX4 ON NUMFLX4.lineItemId=LI.lineItemId AND NUMFLX4.sourceId=LI.sourceId AND NUMFLX4.flexFieldDefRefNum=4\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl NUMFLXN4 ON NUMFLXN4.flexFieldDefDtlId=NUMFLX4.flexFieldDefDtlId AND NUMFLXN4.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmNumFlxFld NUMFLX5 ON NUMFLX5.lineItemId=LI.lineItemId AND NUMFLX5.sourceId=LI.sourceId AND NUMFLX5.flexFieldDefRefNum=5\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl NUMFLXN5 ON NUMFLXN5.flexFieldDefDtlId=NUMFLX5.flexFieldDefDtlId AND NUMFLXN5.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmNumFlxFld NUMFLX6 ON NUMFLX6.lineItemId=LI.lineItemId AND NUMFLX6.sourceId=LI.sourceId AND NUMFLX6.flexFieldDefRefNum=6\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl NUMFLXN6 ON NUMFLXN6.flexFieldDefDtlId=NUMFLX6.flexFieldDefDtlId AND NUMFLXN6.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmNumFlxFld NUMFLX7 ON NUMFLX7.lineItemId=LI.lineItemId AND NUMFLX7.sourceId=LI.sourceId AND NUMFLX7.flexFieldDefRefNum=7\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl NUMFLXN7 ON NUMFLXN7.flexFieldDefDtlId=NUMFLX7.flexFieldDefDtlId AND NUMFLXN7.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmNumFlxFld NUMFLX8 ON NUMFLX8.lineItemId=LI.lineItemId AND NUMFLX8.sourceId=LI.sourceId AND NUMFLX8.flexFieldDefRefNum=8\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl NUMFLXN8 ON NUMFLXN8.flexFieldDefDtlId=NUMFLX8.flexFieldDefDtlId AND NUMFLXN8.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmNumFlxFld NUMFLX9 ON NUMFLX9.lineItemId=LI.lineItemId AND NUMFLX9.sourceId=LI.sourceId AND NUMFLX9.flexFieldDefRefNum=9\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl NUMFLXN9 ON NUMFLXN9.flexFieldDefDtlId=NUMFLX9.flexFieldDefDtlId AND NUMFLXN9.flexFieldDefSrcId=LI.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLnItmNumFlxFld NUMFLX10 ON NUMFLX10.lineItemId=LI.lineItemId AND NUMFLX10.sourceId=LI.sourceId AND NUMFLX10.flexFieldDefRefNum=10\n");
			sql.append(
					"LEFT OUTER JOIN RDBFlexFieldDefDtl NUMFLXN10 ON NUMFLXN10.flexFieldDefDtlId=NUMFLX10.flexFieldDefDtlId AND NUMFLXN10.flexFieldDefSrcId=LI.sourceId\n");
		} else {

			sql.append(
					"LEFT OUTER JOIN RDBLnItmStrngFlxFld STRFLX ON STRFLX.lineItemId=LI.lineItemId AND STRFLX.sourceId=LI.sourceId AND STRFLX.postingDateRDBId=LI.postingDateRDBId\n");
			sql.append(
					"LEFT OUTER JOIN RDBLnItmDateFlxFld DATEFLX ON DATEFLX.lineItemId=LI.lineItemId AND DATEFLX.sourceId=LI.sourceId AND DATEFLX.postingDateRDBId=LI.postingDateRDBId\n");
			sql.append(
					"LEFT OUTER JOIN RDBLnItmNumFlxFld NUMFLX ON NUMFLX.lineItemId=LI.lineItemId AND NUMFLX.sourceId=LI.sourceId AND NUMFLX.postingDateRDBId=LI.postingDateRDBId\n");
		}

		sql.append(
				"LEFT OUTER JOIN RDBDiscountDim DISCDIM ON LI.discountRDBId!=-1 AND DISCDIM.discountRDBId=LI.discountRDBId\n");
		sql.append("LEFT OUTER JOIN RDBDiscountCat DISCCAT ON DISCCAT.discountCatId=DISCDIM.discountCatId\n");

		sql.append("LEFT OUTER JOIN RDBAssistedState LIASSTSTATE ON LIASSTSTATE.assistedStateId=LI.assistedStateId\n");

		sql.append("LEFT OUTER JOIN RDBBuyer BUYER ON LI.buyerRDBId!=-1 AND BUYER.buyerRDBId=LI.buyerRDBId\n");
		sql.append("LEFT OUTER JOIN RDBPartyType BUYERTYPE ON BUYERTYPE.partyTypeId=BUYER.partyTypeId\n");
		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail BUYERPTY ON BUYERPTY.partyDtlId=BUYER.partyDtlId AND BUYERPTY.partySourceId=BUYER.sourceId\n");

		sql.append(
				"LEFT OUTER JOIN RDBBuyer BUYERCLS ON LI.buyerClassRDBId!=-1 AND BUYERCLS.buyerRDBId=LI.buyerClassRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail BUYERCLSPTY ON BUYERCLS.partyDtlId!=-1 AND BUYERCLSPTY.partyDtlId=BUYERCLS.partyDtlId AND BUYERCLSPTY.partySourceId=BUYERCLS.sourceId\n");

		sql.append("LEFT OUTER JOIN RDBSeller SELLER ON LI.sellerRDBId!=-1 AND SELLER.sellerRDBId=LI.sellerRDBId\n");
		sql.append("LEFT OUTER JOIN RDBPartyType SELLERTYPE ON SELLERTYPE.partyTypeId=SELLER.partyTypeId\n");
		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail SELLERPTY ON SELLERPTY.partyDtlId=SELLER.partyDtlId AND SELLERPTY.partySourceId=SELLER.sourceId\n");

		sql.append(
				"LEFT OUTER JOIN RDBSeller SELLERCLS ON LI.sellerClassRDBId!=-1 AND SELLERCLS.sellerRDBId=LI.sellerClassRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail SELLERCLSPTY ON SELLERCLS.partyDtlId!=-1 AND SELLERCLSPTY.partyDtlId=SELLERCLS.partyDtlId AND SELLERCLSPTY.partySourceId=SELLERCLS.sourceId\n");

		sql.append("LEFT OUTER JOIN RDBOwner OWNER ON LI.ownerRDBId!=-1 AND OWNER.ownerRDBId=LI.ownerRDBId\n");
		sql.append("LEFT OUTER JOIN RDBPartyType OWNERTYPE ON OWNERTYPE.partyTypeId=OWNER.partyTypeId\n");
		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail OWNERPTY ON OWNERPTY.partyDtlId=OWNER.partyDtlId AND OWNERPTY.partySourceId=OWNER.sourceId\n");

		sql.append(
				"LEFT OUTER JOIN RDBOwner OWNERCLS ON LI.ownerClassRDBId!=-1 AND OWNERCLS.ownerRDBId=LI.ownerClassRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail OWNERCLSPTY ON OWNERCLSPTY.partyDtlId=OWNERCLS.partyDtlId AND OWNERCLSPTY.partySourceId=OWNERCLS.sourceId\n");

		sql.append(
				"LEFT OUTER JOIN RDBRecipient RECP ON LI.recipientRDBId!=-1 AND RECP.recipientRDBId=LI.recipientRDBId\n");
		sql.append("LEFT OUTER JOIN RDBPartyType RECPTYPE ON RECPTYPE.partyTypeId=RECP.partyTypeId\n");
		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail RECPPTY ON RECPPTY.partyDtlId=RECP.partyDtlId AND RECPPTY.partySourceId=RECP.sourceId\n");

		sql.append(
				"LEFT OUTER JOIN RDBRecipient RECPCLS ON LI.recpClassRDBId!=-1 AND RECPCLS.recipientRDBId=LI.recpClassRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail RECPCLSPTY ON RECPCLSPTY.partyDtlId=RECPCLS.partyDtlId AND RECPCLSPTY.partySourceId=RECPCLS.sourceId\n");

		sql.append(
				"LEFT OUTER JOIN RDBDispatcher DISP ON LI.dispatcherRDBId!=-1 AND DISP.dispatcherRDBId=LI.dispatcherRDBId\n");
		sql.append("LEFT OUTER JOIN RDBPartyType DISPTYPE ON DISPTYPE.partyTypeId=DISP.partyTypeId\n");
		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail DISPPTY ON DISPPTY.partyDtlId=DISP.partyDtlId AND DISPPTY.partySourceId=DISP.sourceId\n");

		sql.append(
				"LEFT OUTER JOIN RDBDispatcher DISPCLS ON LI.dispClassRDBId!=-1 AND DISPCLS.dispatcherRDBId=LI.dispClassRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail DISPCLSPTY ON DISPCLSPTY.partyDtlId=DISPCLS.partyDtlId AND DISPCLSPTY.partySourceId=DISPCLS.sourceId\n");

		sql.append(
				"LEFT OUTER JOIN RDBCurrencyUnit STATCUR ON LI.statisticalValueCurrencyUnitId!=-1 AND STATCUR.currencyUnitId=LI.statisticalValueCurrencyUnitId\n");

		sql.append("INNER JOIN RDBTaxpayerCode TAXPAYER ON TAXPAYER.taxpayerRDBId=LI.taxpayerRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail TAXPAYERPTY ON TAXPAYERPTY.partyDtlId=TAXPAYER.partyDtlId AND TAXPAYERPTY.partySourceId=TAXPAYER.sourceId\n");

		sql.append(
				"LEFT OUTER JOIN RDBPartyDetail VENDORPTY ON VENDORPTY.partyDtlId=SELLERPTY.partyDtlId AND VENDORPTY.partySourceId=SELLERPTY.partySourceId AND TAXPAYERPTY.partyDtlId=BUYERPTY.partyDtlId AND TAXPAYERPTY.partySourceId=BUYERPTY.partySourceId\n");

		sql.append("LEFT OUTER JOIN RDBDestination DEST ON DEST.lineItemId=LI.lineItemId\n");
		sql.append("LEFT OUTER JOIN RDBTaxAreaJurNames DESTTA ON DESTTA.taxAreaId=DEST.taxAreaId\n");

		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict DESTJURD1 ON DESTJURD1.taxAreaId=DESTTA.taxAreaId AND DESTJURD1.districtId=1 AND DESTJURD1.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict DESTJURD2 ON DESTJURD2.taxAreaId=DESTTA.taxAreaId AND DESTJURD2.districtId=2 AND DESTJURD2.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict DESTJURD3 ON DESTJURD3.taxAreaId=DESTTA.taxAreaId AND DESTJURD3.districtId=3 AND DESTJURD3.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict DESTJURD4 ON DESTJURD4.taxAreaId=DESTTA.taxAreaId AND DESTJURD4.districtId=4 AND DESTJURD4.postingDateRDBId=LI.postingDateRDBId\n");

		sql.append("LEFT OUTER JOIN RDBAdminDest ADMINDEST ON ADMINDEST.lineItemId=LI.lineItemId\n");
		sql.append("LEFT OUTER JOIN RDBTaxAreaJurNames ADMINDESTTA ON ADMINDESTTA.taxAreaId=ADMINDEST.taxAreaId\n");

		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict ADMINDESTJURD1 ON ADMINDESTJURD1.taxAreaId=ADMINDESTTA.taxAreaId AND ADMINDESTJURD1.districtId=1 AND ADMINDESTJURD1.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict ADMINDESTJURD2 ON ADMINDESTJURD2.taxAreaId=ADMINDESTTA.taxAreaId AND ADMINDESTJURD2.districtId=2 AND ADMINDESTJURD2.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict ADMINDESTJURD3 ON ADMINDESTJURD3.taxAreaId=ADMINDESTTA.taxAreaId AND ADMINDESTJURD3.districtId=3 AND ADMINDESTJURD3.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict ADMINDESTJURD4 ON ADMINDESTJURD4.taxAreaId=ADMINDESTTA.taxAreaId AND ADMINDESTJURD4.districtId=4 AND ADMINDESTJURD4.postingDateRDBId=LI.postingDateRDBId\n");

		sql.append("LEFT OUTER JOIN RDBAdminOrigin ADMINORIG ON ADMINORIG.lineItemId=LI.lineItemId\n");
		sql.append("LEFT OUTER JOIN RDBTaxAreaJurNames ADMINORIGTA ON ADMINORIGTA.taxAreaId=ADMINORIG.taxAreaId\n");

		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict ADMINORIGJURD1 ON ADMINORIGJURD1.taxAreaId=ADMINORIGTA.taxAreaId AND ADMINORIGJURD1.districtId=1 AND ADMINORIGJURD1.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict ADMINORIGJURD2 ON ADMINORIGJURD2.taxAreaId=ADMINORIGTA.taxAreaId AND ADMINORIGJURD2.districtId=2 AND ADMINORIGJURD2.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict ADMINORIGJURD3 ON ADMINORIGJURD3.taxAreaId=ADMINORIGTA.taxAreaId AND ADMINORIGJURD3.districtId=3 AND ADMINORIGJURD3.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict ADMINORIGJURD4 ON ADMINORIGJURD4.taxAreaId=ADMINORIGTA.taxAreaId AND ADMINORIGJURD4.districtId=4 AND ADMINORIGJURD4.postingDateRDBId=LI.postingDateRDBId\n");

		sql.append("LEFT OUTER JOIN RDBPhysicalOrigin PHYSORIG ON PHYSORIG.lineItemId=LI.lineItemId\n");
		sql.append("LEFT OUTER JOIN RDBTaxAreaJurNames PHYSORIGTA ON PHYSORIGTA.taxAreaId=PHYSORIG.taxAreaId\n");

		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict PHYSORIGJURD1 ON PHYSORIGJURD1.taxAreaId=PHYSORIGTA.taxAreaId AND PHYSORIGJURD1.districtId=1 AND PHYSORIGJURD1.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict PHYSORIGJURD2 ON PHYSORIGJURD2.taxAreaId=PHYSORIGTA.taxAreaId AND PHYSORIGJURD2.districtId=2 AND PHYSORIGJURD2.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict PHYSORIGJURD3 ON PHYSORIGJURD3.taxAreaId=PHYSORIGTA.taxAreaId AND PHYSORIGJURD3.districtId=3 AND PHYSORIGJURD3.postingDateRDBId=LI.postingDateRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RdbTaxAreaDistrict PHYSORIGJURD4 ON PHYSORIGJURD4.taxAreaId=PHYSORIGTA.taxAreaId AND PHYSORIGJURD4.districtId=4 AND PHYSORIGJURD4.postingDateRDBId=LI.postingDateRDBId\n");

		sql.append("INNER JOIN RDBLineItemTax TAX ON TAX.lineItemId=LI.lineItemId\n");
		sql.append("LEFT OUTER JOIN RDBTaxType TAXTYPE ON TAXTYPE.taxTypeId=TAX.taxTypeId\n");
		sql.append("LEFT OUTER JOIN RDBJurisdiction JUR ON JUR.jurId=TAX.jurisdictionId\n");
		sql.append("LEFT OUTER JOIN RDBJurType JURTYPE ON JURTYPE.jurisdictionTypeId=JUR.jurTypeId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTaxImpsnDetail IMP ON TAX.taxImpsnDtlId!=-1 AND IMP.taxImpsnDtlId=TAX.taxImpsnDtlId AND IMP.taxImpsnSrcId=TAX.taxImpsnSrcId\n");
		sql.append(
				"LEFT OUTER JOIN RDBImpositionType IMPTYPE ON IMPTYPE.impsnTypeId=IMP.impsnTypeId AND IMPTYPE.impsnTypeSrcId=IMP.impsnTypeSrcId\n");

		sql.append("LEFT OUTER JOIN RDBTaxResultType TAXRESULT ON TAXRESULT.taxResultTypeId=TAX.taxResultTypeId\n");
		sql.append("LEFT OUTER JOIN RDBInputOutputType IOTYPE ON IOTYPE.inputOutputTypeId=TAX.inputOutputTypeId\n");

		sql.append(
				"LEFT OUTER JOIN RDBJurisdiction INPTAXISOCTRY ON TAX.inputOutputTypeId=1 AND INPTAXISOCTRY.jurId=TAX.jurisdictionId\n");

		sql.append(
				"LEFT OUTER JOIN RDBCurrencyUnit FILECURUNIT ON TAX.filingCurcyUnitId!=-1 AND FILECURUNIT.currencyUnitId=TAX.filingCurcyUnitId\n");
		sql.append(
				"LEFT OUTER JOIN RDBAssistedState TAXASSTSTATE ON TAXASSTSTATE.assistedStateId=TAX.assistedStateId\n");

		if (version.equals("7.0")) {
			sql.append(
					"LEFT OUTER JOIN RDBLineItemNotice ONOTICE1 ON ONOTICE1.lineItemId=TAX.lineItemId AND ONOTICE1.lineItemTaxId=TAX.lineItemTaxId AND ONOTICE1.ordinal=1\n");
			sql.append(
					"LEFT OUTER JOIN RDBOutputNotice ONOTICEN1 ON ONOTICEN1.outputNoticeId=ONOTICE1.outputNoticeId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLineItemNotice ONOTICE2 ON ONOTICE2.lineItemId=TAX.lineItemId AND ONOTICE2.lineItemTaxId=TAX.lineItemTaxId AND ONOTICE2.ordinal=2\n");
			sql.append(
					"LEFT OUTER JOIN RDBOutputNotice ONOTICEN2 ON ONOTICEN2.outputNoticeId=ONOTICE2.outputNoticeId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLineItemNotice ONOTICE3 ON ONOTICE3.lineItemId=TAX.lineItemId AND ONOTICE3.lineItemTaxId=TAX.lineItemTaxId AND ONOTICE3.ordinal=3\n");
			sql.append(
					"LEFT OUTER JOIN RDBOutputNotice ONOTICEN3 ON ONOTICEN3.outputNoticeId=ONOTICE3.outputNoticeId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLineItemNotice ONOTICE4 ON ONOTICE4.lineItemId=TAX.lineItemId AND ONOTICE4.lineItemTaxId=TAX.lineItemTaxId AND ONOTICE4.ordinal=4\n");
			sql.append(
					"LEFT OUTER JOIN RDBOutputNotice ONOTICEN4 ON ONOTICEN4.outputNoticeId=ONOTICE4.outputNoticeId\n");

			sql.append(
					"LEFT OUTER JOIN RDBLineItemNotice ONOTICE5 ON ONOTICE5.lineItemId=TAX.lineItemId AND ONOTICE5.lineItemTaxId=TAX.lineItemTaxId AND ONOTICE5.ordinal=5\n");
			sql.append(
					"LEFT OUTER JOIN RDBOutputNotice ONOTICEN5 ON ONOTICEN5.outputNoticeId=ONOTICE5.outputNoticeId\n");
		} else {
			sql.append(
					"LEFT OUTER JOIN RDBLineItemNotice ONOTICE ON ONOTICE.lineItemId=TAX.lineItemId AND ONOTICE.lineItemTaxId=TAX.lineItemTaxId AND ONOTICE.sourceId=TAX.sourceId\n");

			sql.append(
					"LEFT OUTER JOIN RDBOutputNotice ONOTICEN1 ON ONOTICEN1.outputNoticeId=ONOTICE.outputNotice1Id\n");
			sql.append(
					"LEFT OUTER JOIN RDBOutputNotice ONOTICEN2 ON ONOTICEN2.outputNoticeId=ONOTICE.outputNotice2Id\n");
			sql.append(
					"LEFT OUTER JOIN RDBOutputNotice ONOTICEN3 ON ONOTICEN3.outputNoticeId=ONOTICE.outputNotice3Id\n");
			sql.append(
					"LEFT OUTER JOIN RDBOutputNotice ONOTICEN4 ON ONOTICEN4.outputNoticeId=ONOTICE.outputNotice4Id\n");
			sql.append(
					"LEFT OUTER JOIN RDBOutputNotice ONOTICEN5 ON ONOTICEN5.outputNoticeId=ONOTICE.outputNotice5Id\n");
		}

		sql.append("LEFT OUTER JOIN RDBApportionmentType APPTYPE ON APPTYPE.apportionTypeId=TAX.apportionTypeId\n");
		sql.append(
				"LEFT OUTER JOIN RDBCertificateDim CERT ON TAX.certificateRDBId!=-1 AND CERT.certificateRDBId=TAX.certificateRDBId\n");
		sql.append("LEFT OUTER JOIN RDBCertClassType CERTTYPE ON CERTTYPE.certClassTypeId=CERT.certClassTypeId\n");

		sql.append(
				"LEFT OUTER JOIN RDBPartyRoleType TAXRESPROLETYPE ON TAX.taxResponsibilityRoleTypeId!=-1 AND TAXRESPROLETYPE.partyRoleTypeId=TAX.taxResponsibilityRoleTypeId\n");

		sql.append("LEFT OUTER JOIN RDBBuyerRegistration BUYERREG ON BUYERREG.buyerRegRDBId=TAX.buyerRegRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBDispRegistration DISPREG ON DISPREG.dispatcherRegRDBId=TAX.dispatcherRegRDBId\n");
		sql.append("LEFT OUTER JOIN RDBOwnerRegistration OWNERREG ON OWNERREG.ownerRegRDBId=TAX.ownerRegRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBRecipientRegistration RECPREG ON RECPREG.recipientRegRDBId=TAX.recipientRegRDBId\n");
		sql.append("LEFT OUTER JOIN RDBSellerRegistration SELLERREG ON SELLERREG.sellerRegRDBId=TAX.sellerRegRDBId\n");
		sql.append(
				"LEFT OUTER JOIN RDBTaxpayerRegistration TAXPAYERREG ON TAXPAYERREG.taxpayerRegRDBId=TAX.taxpayerRegRDBId\n");

		sql.append(
				"LEFT OUTER JOIN RDBTaxDetailType TAXDETTYPE ON TAXDETTYPE.lineItemTxDtlTypId=TAX.taxDetailTypeId\n");
		sql.append("LEFT OUTER JOIN RDBReasonCategory REASONCAT ON REASONCAT.reasonCategoryId=TAX.reasonCategoryId\n");
		sql.append(
				"LEFT OUTER JOIN RDBFilingCategory FILECAT ON TAX.filingCategoryId!=-1 AND FILECAT.filingCategoryId=TAX.filingCategoryId\n");
		sql.append(
				"LEFT OUTER JOIN RDBRateClass RATECLASS ON TAX.rateClassId!=-1 AND RATECLASS.rateClassId=TAX.rateClassId\n");

		if (Column.versionApplies("8.0", version)) {

			sql.append(
					"LEFT OUTER JOIN RDBTxbltyDvrDim MATORG ON LI.materialOriginRDBId!=-1 AND MATORG.txbltyDvrRDBId=LI.materialOriginRDBId\n");
			sql.append(
					"LEFT OUTER JOIN RDBTxbltyDvrDetail MATORGN ON MATORGN.txbltyDvrDtlId=MATORG.txbltyDvrDtlId AND MATORGN.txbltyDvrSrcId=MATORG.txbltyDvrSrcId\n");

			sql.append("LEFT OUTER JOIN RDBCurrencyUnit CCCU ON CCCU.currencyUnitId=LI.companyCodeCurrencyUnitId\n");
		}

		// sql.append("WHERE LI.postingDateRDBId=?");

		return sql.toString();
	}

}
