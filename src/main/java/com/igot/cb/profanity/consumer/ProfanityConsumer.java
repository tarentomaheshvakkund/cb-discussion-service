package com.igot.cb.profanity.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.discussion.entity.DiscussionAnswerPostReplyEntity;
import com.igot.cb.discussion.entity.DiscussionEntity;
import com.igot.cb.discussion.repository.DiscussionAnswerPostReplyRepository;
import com.igot.cb.discussion.repository.DiscussionRepository;
import com.igot.cb.discussion.service.AnswerPostReplyService;
import com.igot.cb.discussion.service.DiscussionService;
import com.igot.cb.notificationUtill.HelperMethodService;
import com.igot.cb.notificationUtill.NotificationTriggerService;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.DiscussionServiceUtil;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.igot.cb.pores.util.Constants.*;

@Component
@Slf4j
public class ProfanityConsumer {

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private DiscussionRepository discussionRepository;

    @Autowired
    private EsUtilService esUtilService;

    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DiscussionService discussionService;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    @Qualifier(Constants.SEARCH_RESULT_REDIS_TEMPLATE)
    private RedisTemplate<String, SearchResult> redisTemplate;

    @Autowired
    private DiscussionAnswerPostReplyRepository discussionAnswerPostReplyRepository;

    @Autowired
    private NotificationTriggerService notificationTriggerService;

    @Autowired
    private HelperMethodService helperMethodService;

    @Autowired
    private AnswerPostReplyService answerPostReplyService;

    /**
     * Consumes messages from the Kafka topic for profanity checks on text content.
     * It processes the text data, checks if it contains profane content, and updates
     * the discussion or answer post reply entities accordingly.
     *
     * @param textData the Kafka message containing text data to be checked for profanity
     */
    @KafkaListener(topics = "${kafka.topic.process.check.content.profanity}", groupId = "${kafka.group.process.check.content.profanity}")
    public void checkTextContentIsProfane(ConsumerRecord<String, String> textData) {
        if (StringUtils.hasText(textData.value())) {
            try {
                JsonNode textDataNode = mapper.readTree(textData.value());
                String discussionId = textDataNode.path(Constants.REQUEST_DATA).path(Constants.METADATA).path(Constants.POST_ID).asText();
                String parentDiscussionId = extractFieldAsText(
                        textDataNode, Constants.REQUEST_DATA, Constants.METADATA, Constants.PARENT_DISCUSSION_ID);
                String parentAnswerPostId = extractFieldAsText(
                        textDataNode, Constants.REQUEST_DATA, Constants.METADATA, Constants.PARENT_ANSWER_POST_ID);
                String type = textDataNode.path(Constants.REQUEST_DATA).path(Constants.METADATA).path(Constants.TYPE).asText();
                boolean isProfane = textDataNode.path(Constants.RESPONSE_DATA).path(Constants.RESPONSE_DATA_PATH).path(Constants.IS_PROFANE).asBoolean(false);
                String profanityResponseJson = textDataNode.toString();
                CompletableFuture.runAsync(() -> {
                    try {
                        updateProfanityFieldsAndSync(type, discussionId, profanityResponseJson, isProfane, parentDiscussionId, parentAnswerPostId);
                    } catch (Exception ex) {
                        log.error("Failed to update profanity fields for Discussion: {}", discussionId, ex);
                        if (Constants.QUESTION.equalsIgnoreCase(type) || Constants.ANSWER_POST.equalsIgnoreCase(type)) {
                            discussionRepository.updateProfanityCheckStatusByDiscussionId(discussionId, Constants.PROFANITY_CHECK_UPDATE_FAILED, false);
                        } else if (Constants.ANSWER_POST_REPLY.equalsIgnoreCase(type)) {
                            discussionAnswerPostReplyRepository.updateProfanityCheckStatusByDiscussionId(discussionId, Constants.PROFANITY_CHECK_UPDATE_FAILED, false);
                        }
                    }
                });
            } catch (JsonProcessingException e) {
                log.error("Failed to parse JSON from Kafka message: {}", textData.value(), e);
            }
        }
    }

    /**
     * Sync the profane details to Elasticsearch and deletes relevant caches.
     *
     * @param discussionId       the ID of the discussion
     * @param isProfane          indicates whether the discussion is profane
     * @param type               - the type of the post (e.g., question, answerPost)
     * @param parentDiscussionId the ID of the parent discussion, if applicable
     */
    private void syncProfaneDetailsToESForDiscussion(String discussionId, boolean isProfane, String type, String parentDiscussionId) {
        Optional<DiscussionEntity> discussionEntity = discussionRepository.findById(discussionId);
        if (discussionEntity.isPresent()) {
            DiscussionEntity discussionDbData = discussionEntity.get();
            if (Boolean.FALSE.equals(discussionDbData.getIsActive())) {
                log.info("Discussion is inactive, skipping Elasticsearch update for PostId: {}", discussionId);
            } else {
                ObjectNode data = (ObjectNode) discussionDbData.getData();
                Map<String, Object> map = objectMapper.convertValue(data, new TypeReference<Map<String, Object>>() {
                });
                map.put(Constants.IS_PROFANE, isProfane);
                esUtilService.updateDocument(cbServerProperties.getDiscussionEntity(), discussionDbData.getDiscussionId(), map, cbServerProperties.getElasticDiscussionJsonPath());
                if (isProfane) handleProfanityForDiscussionAnswerPostCreation(type, parentDiscussionId, discussionDbData, data);
            }
        } else {
            log.warn("Discussion not found for Discussion Id: {}", discussionId);
        }
    }

    /**
     * Sync the profane details to Elasticsearch for an answer post reply and deletes relevant caches.
     *
     * @param discussionId       the ID of the discussion answer post reply
     * @param isProfane          indicates whether the answer post reply is profane
     * @param parentDiscussionId the ID of the parent discussion, if applicable
     * @param parentAnswerPostId the ID of the parent answer post, if applicable
     */
    private void syncProfaneDetailsToESForAnswerPost(String discussionId, boolean isProfane, String parentDiscussionId, String parentAnswerPostId) {
        Optional<DiscussionAnswerPostReplyEntity> discussionAnswerPostReplyEntity = discussionAnswerPostReplyRepository.findById(discussionId);
        if (discussionAnswerPostReplyEntity.isPresent()) {
            DiscussionAnswerPostReplyEntity discussionAnswerPostReply = discussionAnswerPostReplyEntity.get();
            if (Boolean.FALSE.equals(discussionAnswerPostReply.getIsActive())) {
                log.info("Discussion Answer Post Reply is inactive, skipping Elasticsearch update for PostId: {}", discussionId);
            } else {
                ObjectNode data = (ObjectNode) discussionAnswerPostReply.getData();
                Map<String, Object> map = objectMapper.convertValue(data, new TypeReference<Map<String, Object>>() {
                });
                map.put(Constants.IS_PROFANE, isProfane);
                esUtilService.updateDocument(cbServerProperties.getDiscussionEntity(), discussionAnswerPostReply.getDiscussionId(), map, cbServerProperties.getElasticDiscussionJsonPath());
                if (isProfane) {
                    log.info("Profanity detected in answer post reply: {}", discussionId);
                    handleProfanityForAnswerPostReplyCreation(parentDiscussionId, parentAnswerPostId, discussionAnswerPostReply, data);
                    Optional<DiscussionEntity> discussionEntity = discussionRepository.findById(parentAnswerPostId);
                    discussionEntity.ifPresent(discussionDbData -> {
                        discussionDbData.setIsProfane(false);
                        answerPostReplyService.updateAnswerPostReplyToAnswerPost(discussionDbData, discussionId, DECREMENT);
                    });
                }
            }
        } else {
            log.warn("Discussion Answer Post Reply not found for Discussion Id: {}", discussionId);
        }
    }


    /**
     * Extracts a field from a JsonNode at the specified path and returns it as text.
     *
     * @param node the JsonNode to extract the field from
     * @param path the path to the field
     * @return the field value as text, or null if not found or is missing/null
     */
    private String extractFieldAsText(JsonNode node, String... path) {
        JsonNode current = node;
        for (String p : path) {
            if (current == null) return null;
            current = current.path(p);
        }
        return (current != null && !current.isMissingNode() && !current.isNull()) ? current.asText() : null;
    }

    /**
     * Updates the profanity fields in the database and synchronizes the changes to Elasticsearch.
     *
     * @param type                  the type of the discussion (e.g., QUESTION, ANSWER_POST, ANSWER_POST_REPLY)
     * @param discussionId          the ID of the discussion
     * @param profanityResponseJson the JSON response containing profanity check results
     * @param isProfane             indicates whether the content is profane
     * @param parentDiscussionId    the ID of the parent discussion, if applicable
     * @param parentAnswerPostId    the ID of the parent answer post, if applicable
     */
    private void updateProfanityFieldsAndSync(String type, String discussionId, String profanityResponseJson, boolean isProfane, String parentDiscussionId, String parentAnswerPostId) {
        if (Constants.QUESTION.equalsIgnoreCase(type) || Constants.ANSWER_POST.equalsIgnoreCase(type)) {
            discussionRepository.updateProfanityFieldsByDiscussionId(discussionId, profanityResponseJson, isProfane, Constants.PROFANITY_CHECK_PASSED);
            log.info("Successfully updated profanity fields for Discussion: {}", discussionId);
            syncProfaneDetailsToESForDiscussion(discussionId, isProfane, type, parentDiscussionId);
        } else if (Constants.ANSWER_POST_REPLY.equalsIgnoreCase(type)) {
            discussionAnswerPostReplyRepository.updateProfanityFieldsByDiscussionId(discussionId, profanityResponseJson, isProfane, Constants.PROFANITY_CHECK_PASSED);
            log.info("Successfully updated profanity fields for Answer Post Reply: {}", discussionId);
            syncProfaneDetailsToESForAnswerPost(discussionId, isProfane, parentDiscussionId, parentAnswerPostId);
        }
    }

    /**
     * Handles the case where profanity is detected in a discussion or answer post creation.
     *
     * @param type               the type of the discussion (e.g., QUESTION, ANSWER_POST)
     * @param parentDiscussionId the ID of the parent discussion, if applicable
     * @param discussionDbData   the discussion data from the database
     * @param data               the data from the discussion
     */
    private void handleProfanityForDiscussionAnswerPostCreation(String type, String parentDiscussionId, DiscussionEntity discussionDbData, ObjectNode data) {
       log.info("Handling profanity for type: {}, discussionId: {}, parentDiscussionId: {}", type, data.get(Constants.DISCUSSION_ID).asText(), parentDiscussionId);
        String userId = discussionDbData.getData().get("createdBy").asText();
        String firstName = helperMethodService.fetchUserFirstName(userId);
        Map<String, Object> notificationData = Map.of(
                Constants.COMMUNITY_ID, data.get(Constants.COMMUNITY_ID).asText(),
                Constants.DISCUSSION_ID, data.get(Constants.DISCUSSION_ID).asText(),
                IS_PROFANE, true
        );
        if (Constants.QUESTION.equalsIgnoreCase(type)) {
            log.info("Profanity detected in question post: {}", data.get(Constants.DISCUSSION_ID).asText());
            notificationTriggerService.triggerNotification(Constants.PROFANITY_CHECK, ALERT, Collections.singletonList(userId), TITLE, firstName, notificationData);
            discussionService.deleteCacheByCommunity(Constants.DISCUSSION_CACHE_PREFIX + data.get(Constants.COMMUNITY_ID).asText());
            discussionService.deleteCacheByCommunity(Constants.DISCUSSION_POSTS_BY_USER + data.get(Constants.COMMUNITY_ID).asText() + Constants.UNDER_SCORE + userId);
            discussionService.updateCacheForFirstFivePages(data.get(Constants.COMMUNITY_ID).asText(), false);
        } else if (Constants.ANSWER_POST.equalsIgnoreCase(type) && org.apache.commons.lang3.StringUtils.isNotEmpty(parentDiscussionId)) {
            log.info("Profanity detected in answer post: {}", data.get(Constants.DISCUSSION_ID).asText());
            notificationTriggerService.triggerNotification(Constants.PROFANITY_CHECK, ALERT, Collections.singletonList(userId), TITLE, firstName, notificationData);
            discussionService.deleteCacheByCommunity(Constants.DISCUSSION_CACHE_PREFIX + data.get(Constants.COMMUNITY_ID).asText());
            discussionService.updateCacheForFirstFivePages(data.get(Constants.COMMUNITY_ID).asText(), false);
            redisTemplate.opsForValue()
                    .getAndDelete(DiscussionServiceUtil.generateRedisJwtTokenKey(discussionService.createSearchCriteriaWithDefaults(
                            parentDiscussionId,
                            data.get(Constants.COMMUNITY_ID).asText(),
                            Constants.ANSWER_POST)));
            Optional<DiscussionEntity> discussionEntity = discussionRepository.findById(parentDiscussionId);
            discussionEntity.ifPresent(discussionDbUpdateData -> {
                discussionDbUpdateData.setIsProfane(false);
                discussionService.updateAnswerPostToDiscussion(discussionDbUpdateData, data.get(Constants.DISCUSSION_ID).asText(), DECREMENT);
            });
        }
    }

    /**
     * Handles the case where profanity is detected in an answer post reply creation.
     *
     * @param parentDiscussionId        the ID of the parent discussion
     * @param parentAnswerPostId        the ID of the parent answer post
     * @param discussionAnswerPostReply the discussion answer post reply entity
     * @param data                      the data from the discussion answer post reply
     */
    private void handleProfanityForAnswerPostReplyCreation(String parentDiscussionId, String parentAnswerPostId, DiscussionAnswerPostReplyEntity discussionAnswerPostReply, ObjectNode data) {
        log.info("Handling profanity for answer post reply: {}, parentDiscussionId: {}, parentAnswerPostId: {}",
                data.get(Constants.DISCUSSION_ID).asText(), parentDiscussionId, parentAnswerPostId);
        String userId = discussionAnswerPostReply.getData().get("createdBy").asText();
        String firstName = helperMethodService.fetchUserFirstName(userId);
        Map<String, Object> notificationData = Map.of(
                Constants.COMMUNITY_ID, data.get(Constants.COMMUNITY_ID).asText(),
                Constants.DISCUSSION_ID, data.get(Constants.DISCUSSION_ID).asText(),
                IS_PROFANE, true
        );
        notificationTriggerService.triggerNotification(Constants.PROFANITY_CHECK, ALERT, Collections.singletonList(userId), TITLE, firstName, notificationData);
        redisTemplate.opsForValue()
                .getAndDelete(DiscussionServiceUtil.generateRedisJwtTokenKey(answerPostReplyService.createDefaultSearchCriteria(
                        parentAnswerPostId,
                        data.get(Constants.COMMUNITY_ID).asText())));
        redisTemplate.opsForValue()
                .getAndDelete(DiscussionServiceUtil.generateRedisJwtTokenKey(discussionService.createSearchCriteriaWithDefaults(
                        parentDiscussionId,
                        data.get(Constants.COMMUNITY_ID).asText(),
                        Constants.ANSWER_POST)));
    }
}
