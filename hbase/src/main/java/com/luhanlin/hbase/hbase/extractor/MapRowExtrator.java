package com.luhanlin.hbase.hbase.extractor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MapRowExtrator implements RowExtractor<Map<String,String>>,Serializable {
	
	private static final long serialVersionUID = 1543027485077396235L;
	
	private Map<String,String> row;

	/* (non-Javadoc)
	 * @see com.bh.d406.bigdata.hbase.extractor.RowExtractor#extractRowData(org.apache.hadoop.hbase.client.Result, int)
	 */
	@Override
	public Map<String, String> extractRowData(Result result, int rowNum)
			throws IOException {
		
		row = new HashMap<String,String>();

		List<Cell> cells = result.listCells();

		cells.forEach(x->{
			System.out.println(x);
		});


		for(Cell cell :  result.listCells()) {
			row.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
		}
		return row;
	}

}
