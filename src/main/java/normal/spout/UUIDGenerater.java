package normal.spout;

import java.util.UUID;

public class UUIDGenerater {

	/** 
	* @Title: generateUUId 
	* @Description: TODO(生成不带-的UUID)
	* @return UUID
	* @throws TODO(无异常抛出)
	*/
	public static String generateUUId() {
		return UUID.randomUUID().toString().replace("-", "");
	}
	
	
}
