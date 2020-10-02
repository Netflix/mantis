//package io.mantisrx.server.master.persistence;
////package com.netflix.mantis.master.persistence;
////
//import akka.actor.AbstractActor;
//import akka.actor.ActorRef;
//import akka.actor.Props;
//import akka.event.Logging;
//import akka.event.LoggingAdapter;
//import io.mantisrx.master.jobcluster.job.IMantisJobMetadata;
//import io.mantisrx.master.jobcluster.proto.BaseRequest;
//import io.mantisrx.master.jobcluster.proto.BaseResponse;
//
//import java.util.List;
//
//import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.CLIENT_ERROR;
//import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SERVER_ERROR;
//import static io.mantisrx.master.jobcluster.proto.BaseResponse.ResponseCode.SUCCESS;
//
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.beust.jcommander.internal.Lists;
////
//public class ArchivedJobsLoaderActor extends AbstractActor {
////
//	private final Logger logger = LoggerFactory.getLogger(ArchivedJobsLoaderActor.class);
//	private final MantisJobStore jobStore;
////	
////	private Map<String, JobClusterDAO> jobClusterMap = new HashMap<>();
////	private Map<String, JobDAO> jobMap = new HashMap<>();
////	
//	public static Props props(final MantisJobStore jobStore) {
//		return Props.create(ArchivedJobsLoaderActor.class, jobStore);
//	}
////
//	public ArchivedJobsLoaderActor(final MantisJobStore jobStore) {
//		this.jobStore = jobStore;
//	}
////
//	public static class LoadArchivedJobsRequest extends BaseRequest {
//		
//		public LoadArchivedJobsRequest() {
//			super();
//		}
//	}
//
//
//	public static class LoadArchivedJobsResponse extends BaseResponse {
//	    public final List<IMantisJobMetadata> archivedJobsList;
//		public LoadArchivedJobsResponse(long requestId, ResponseCode isSuccess, String message, List<IMantisJobMetadata> archivedList) {
//			super(requestId, isSuccess, message);
//			this.archivedJobsList = archivedList;
//		}
//	}
//	
//	private void onLoadArchivedJobs(final LoadArchivedJobsRequest request) {
//	    ActorRef sender = getSender();
//	    try {
//    	    List<IMantisJobMetadata> archivedList = jobStore.loadAllArchivedJobs();
//    	    sender.tell(new LoadArchivedJobsResponse(request.requestId, SUCCESS, "Loaded " + archivedList.size() + " archived jobs", archivedList), sender);
//	    } catch(Exception e) {
//	        sender.tell(new LoadArchivedJobsResponse(request.requestId, SERVER_ERROR, "Error loading archived jobs  " + e.getMessage(), Lists.newArrayList(0)), sender);
//	    }
//	}
//	@Override
//	public void preStart() throws Exception {
//		logger.info("Persistence Actor started");
//	}
//
//	@Override
//	public void postStop() throws Exception {
//		logger.info("Persistence Actor stopped");
//	}
//
//	@Override
//	public Receive createReceive() {
//		return receiveBuilder()
//				.match(LoadArchivedJobsRequest.class, r -> onLoadArchivedJobs(r))
//				.matchAny(x -> logger.info("unexpected message '{}' received by ArchivedJobsLoaderActor actor ", x))
//				.build();
//	}
//}
