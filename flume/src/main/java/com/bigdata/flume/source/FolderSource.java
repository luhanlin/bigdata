package com.bigdata.flume.source;


import com.bigdata.flume.constant.FlumeConfConstant;
import com.bigdata.flume.fields.MapFields;
import com.bigdata.flume.utils.FileUtilsStronger;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;


public class FolderSource extends AbstractSource implements Configurable, PollableSource {

    private final Logger logger = Logger.getLogger(FolderSource.class);
    //以下为配置在flume.conf文件中
    private String dirStr;
    private String[] dirs;         // 读取的文件目录,以","分隔 //在flume.conf里面配置
    private String successfile;    // 处理成功的数据处理完写入的文件目录
    private long sleeptime = 5;      //睡眠时间
    private int filenum = 500;       //每批文件数量
    //以下为配置在txtparse.properties文件中
    //读取的所有文件集合
    private Collection<File> allFiles;
    //一批处理的文件大小
    private List<File> listFiles;
    private ArrayList<Event> eventList = new ArrayList<Event>();

    @Override
    public void configure(Context context) {
        logger.info("开始初始化flume参数");
        initFlumeParams(context);
        logger.info("初始化flume参数成功");
    }

    @Override
    public Status process() {
        try {
            Thread.currentThread().sleep(sleeptime * 1000);
        } catch (InterruptedException e) {
            logger.error(null, e);
        }

        Status status = null;
        try {
            // for (String dir : dirs) {
            logger.info("dirStr===========" + dirStr);
            allFiles = FileUtils.listFiles(new File(dirStr), new String[]{"txt", "bcp"}, true);
            if (allFiles.size() >= filenum) {
                listFiles = ((List<File>) allFiles).subList(0, filenum);
            } else {
                listFiles = ((List<File>) allFiles);
            }

            if (listFiles.size() > 0) {
                for (File file : listFiles) {
                    String fileName = file.getName();
                    Map<String, Object> stringObjectMap = FileUtilsStronger.parseFile(file, successfile);
                    String absoluteFilename = (String) stringObjectMap.get(MapFields.ABSOLUTE_FILENAME);
                    List<String> lines = (List<String>) stringObjectMap.get(MapFields.VALUE);

                    if (lines != null && lines.size() > 0) {

                        for (String line : lines) {
                            Map<String, String> map = new HashMap<String, String>();
                            map.put(MapFields.FILENAME, fileName);
                            map.put(MapFields.ABSOLUTE_FILENAME, absoluteFilename);
                            SimpleEvent event = new SimpleEvent();
                            byte[] bytes = line.getBytes();
                            event.setBody(bytes);
                            event.setHeaders(map);
                            eventList.add(event);
                        }
                    }

                    try {
                        if (eventList.size() > 0) {
                            ChannelProcessor channelProcessor = getChannelProcessor();
                            channelProcessor.processEventBatch(eventList);
                            logger.info("批量推送到 拦截器 数据大小为" + eventList.size());
                        }
                        eventList.clear();
                    } catch (Exception e) {
                        eventList.clear();
                        logger.error("发送数据到channel失败", e);
                    } finally {
                        eventList.clear();
                    }
                }
            }
            // }
            status = Status.READY;
            return status;
        } catch (Exception e) {
            status = Status.BACKOFF;
            logger.error("异常", e);
            return status;
        }
    }

    /**
     * 初始化flume參數
     *
     * @param context
     */
    public void initFlumeParams(Context context) {
        try {
            //文件处理目录
            dirStr = context.getString(FlumeConfConstant.DIRS);
            dirs = dirStr.split(",");
            //成功处理的文件
            successfile = context.getString(FlumeConfConstant.SUCCESSFILE);
            filenum = context.getInteger(FlumeConfConstant.FILENUM);
            sleeptime = context.getLong(FlumeConfConstant.SLEEPTIME);
            logger.info("dirStr============" + dirStr);
            logger.info("dirs==============" + dirs);
            logger.info("successfile=======" + successfile);
            logger.info("filenum===========" + filenum);
            logger.info("sleeptime=========" + sleeptime);

        } catch (Exception e) {
            logger.error("初始化flume参数失败", e);
        }
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

}