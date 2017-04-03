package org.ekstep.content.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ekstep.content.concrete.processor.AssetsValidatorProcessor;
import org.ekstep.content.concrete.processor.EmbedControllerProcessor;
import org.ekstep.content.concrete.processor.GlobalizeAssetProcessor;
import org.ekstep.content.concrete.processor.LocalizeAssetProcessor;
import org.ekstep.content.concrete.processor.MissingAssetValidatorProcessor;
import org.ekstep.content.concrete.processor.MissingControllerValidatorProcessor;
import org.ekstep.content.processor.AbstractProcessor;
import org.ekstep.content.processor.ContentPipelineProcessor;

public class PipelineRequestorClient {
	
	private static Logger LOGGER = LogManager.getLogger(PipelineRequestorClient.class.getName());
	
	public static AbstractProcessor getPipeline(String operation, String basePath, String contentId) {
		ContentPipelineProcessor contentPipeline = new ContentPipelineProcessor();
		
		AbstractProcessor localizeAssetProcessor = new LocalizeAssetProcessor(basePath, contentId);
		AbstractProcessor embedControllerProcessor = new EmbedControllerProcessor(basePath, contentId);
		AbstractProcessor missingAssetValidatorProcessor = new MissingAssetValidatorProcessor(basePath, contentId);
		AbstractProcessor missingCtrlValidatorProcessor = new MissingControllerValidatorProcessor(basePath, contentId);
		AbstractProcessor assetsValidatorProcessor = new AssetsValidatorProcessor(basePath, contentId);
		AbstractProcessor globalizeAssetProcessor = new GlobalizeAssetProcessor(basePath, contentId);
		
		switch (operation) {
		case "compress":
		case "COMPRESS":
			LOGGER.info("Registering the Processors for 'COMPRESS' Operation.");
			contentPipeline.registerProcessor(localizeAssetProcessor);
			contentPipeline.registerProcessor(missingAssetValidatorProcessor);
			break;
			
		case "extract":
		case "EXTRACT":
			LOGGER.info("Registering the Processors for 'EXTRACT' Operation.");
			contentPipeline.registerProcessor(missingAssetValidatorProcessor);
			contentPipeline.registerProcessor(assetsValidatorProcessor);
			contentPipeline.registerProcessor(missingCtrlValidatorProcessor);
			contentPipeline.registerProcessor(globalizeAssetProcessor);
			contentPipeline.registerProcessor(embedControllerProcessor);
			break;
			
		case "validate":
		case "VALIDATE":
			LOGGER.info("Registering the Processors for 'VALIDATE' Operation.");
			contentPipeline.registerProcessor(localizeAssetProcessor);
			contentPipeline.registerProcessor(missingAssetValidatorProcessor);
			contentPipeline.registerProcessor(assetsValidatorProcessor);
			contentPipeline.registerProcessor(missingCtrlValidatorProcessor);
			break;

		default:
			LOGGER.warn("Invalid Pipeline Operation.");
			break;
		}
		
		return contentPipeline;
	}
	
	public static AbstractProcessor getPipelineChain(String operation, String basePath, String contentId) {
		AbstractProcessor head = null;
		
		AbstractProcessor localizeAssetProcessor = new LocalizeAssetProcessor(basePath, contentId);
		AbstractProcessor embedControllerProcessor = new EmbedControllerProcessor(basePath, contentId);
		AbstractProcessor missingAssetValidatorProcessor = new MissingAssetValidatorProcessor(basePath, contentId);
		AbstractProcessor missingCtrlValidatorProcessor = new MissingControllerValidatorProcessor(basePath, contentId);
		AbstractProcessor assetsValidatorProcessor = new AssetsValidatorProcessor(basePath, contentId);
		AbstractProcessor globalizeAssetProcessor = new GlobalizeAssetProcessor(basePath, contentId);
		
		switch (operation) {
		case "compress":
		case "COMPRESS":
			LOGGER.info("Initialising the Processor's Chain for 'COMPRESS' Operation.");
			localizeAssetProcessor.setNextProcessor(embedControllerProcessor);
			embedControllerProcessor.setNextProcessor(missingAssetValidatorProcessor);
			head = localizeAssetProcessor;
			break;
			
		case "extract":
		case "EXTRACT":
			LOGGER.info("Initialising the Processor's Chain for 'EXTRACT' Operation.");
			missingAssetValidatorProcessor.setNextProcessor(assetsValidatorProcessor);
			assetsValidatorProcessor.setNextProcessor(missingCtrlValidatorProcessor);
			missingCtrlValidatorProcessor.setNextProcessor(globalizeAssetProcessor);
			globalizeAssetProcessor.setNextProcessor(embedControllerProcessor);
			head = missingAssetValidatorProcessor;
			break;
			
		case "validate":
		case "VALIDATE":
			LOGGER.info("Initialising the Processor's Chain for 'VALIDATE' Operation.");
			missingAssetValidatorProcessor.setNextProcessor(assetsValidatorProcessor);
			assetsValidatorProcessor.setNextProcessor(missingCtrlValidatorProcessor);
			head = missingAssetValidatorProcessor;
			break;

		default:
			LOGGER.warn("Invalid Pipeline Operation.");
			break;
		}
		
		return head;
	}

}