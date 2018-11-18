package com.vertex.wfextraction.processor;

import org.springframework.batch.item.ItemProcessor;

import com.vertex.wfextraction.domain.Vertex;

public class ExtractItemProcessor implements ItemProcessor<Vertex, Vertex> {

	@Override
	public Vertex process(Vertex item) throws Exception {

		System.out.println("Item values are " + item.getInputExtendedPrice() 
		+ " " + item.getSourceId());
		return item;
	}

}
