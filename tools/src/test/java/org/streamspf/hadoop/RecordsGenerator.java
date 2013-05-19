package org.streamspf.hadoop;

import java.util.Random;

class RecordsGenerator {

	private Random random = new Random();

	CallRecordDetail randomRecord(CallRecordDetail lastRecord) {
		CallRecordDetail callRecordDetail = new CallRecordDetail();

		callRecordDetail.setTimestamp(lastRecord == null ? System.currentTimeMillis() : lastRecord.getTimestamp());
		callRecordDetail.setDesc(lastRecord == null ? Long.MAX_VALUE : lastRecord.getDesc() - 1L);

		callRecordDetail.setSeconds(this.random.nextInt(1000));
		callRecordDetail.setCalling(this.random.nextBoolean());
		callRecordDetail.setCalleeNumber(13910010000L + this.random.nextInt(10000));
		callRecordDetail.setLocation("??");
		callRecordDetail.setCallType("????");
		callRecordDetail.setCallFee(this.random.nextInt(10000));
		callRecordDetail.setOtherFee(0);
		callRecordDetail.setSubtotal(callRecordDetail.getCallFee() + callRecordDetail.getOtherFee());

		return callRecordDetail;
	}
}
