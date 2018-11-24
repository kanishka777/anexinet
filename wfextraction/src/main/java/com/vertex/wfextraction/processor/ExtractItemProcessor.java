package com.vertex.wfextraction.processor;

import org.springframework.batch.item.ItemProcessor;

public class ExtractItemProcessor implements ItemProcessor<String, String> {

	@Override
	public String process(String item) throws Exception {
		System.out.println("Item row is" + item);
		return item;
	}

}
