package org.streamspf.hadoop.cdr;

import org.streamspf.hadoop.hbase.DefaultHBaseDao;
import org.streamspf.hadoop.hbase.HBaseDao;
import org.streamspf.hadoop.util.CommonUtil;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DRLoad {

    public static void main(String[] args) {
        if (args.length == 0) {
            throw new RuntimeException("请输入文件所在的目录");
        }

        if ((args[0] == null) || (args[0].length() == 0)) {
            throw new RuntimeException("请输入文件所在的目录");
        }

        File dir = new File(args[0]);

        if (!dir.exists()) {
            throw new RuntimeException("输入的目录不存在");
        }

        File[] files = dir.listFiles();
        List<File> dirs = new ArrayList<File>();
        for (File fileName : files) {
            if (fileName.isDirectory()) {
                dirs.add(fileName);
            }
        }

        DRLoad main = new DRLoad();
        main.exec(dirs);
    }

    public void exec(List<File> dirs) {
        for (File file : dirs) {
            HBaseDao dao = new DefaultHBaseDao();
            ReadFile rf = new ReadFile(file, dao);
            Thread thread = new Thread(rf);
            thread.start();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }
    }

    public void readFile(File file, HBaseDao dao) {
        Writer writer = null;
        BufferedWriter bw = null;
        FileReader fr = null;
        BufferedReader br = null;
        long count = 0L;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(Thread.currentThread().getName() + ", file.name = " + file.getName() + ", " + format.format(new Date()));
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String fileName = file.getName();

            String temp;
            List<DetailedListCdr> list = new ArrayList<DetailedListCdr>();
            while ((temp = br.readLine()) != null) {
                list.add(packCdr(temp, fileName));
                if (list.size() >= CommonUtil.count) {
                    count += list.size();
                    try {
                        dao.put(list);
                        list.clear();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            if (list.size() != 0) {
                System.out.println("second times commit = " + list.size());
                count += list.size();
                try {
                    dao.put(list);
                    list.clear();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            System.out.println(Thread.currentThread().getName() + ", file.name = " + file.getName() +
                    ", " + format.format(new Date()) + ", count = " + count);

            br.close();
            fr.close();
        } catch (Exception e) {
            e.printStackTrace();
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }

            if (fr != null) {
                try {
                    fr.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }

            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }

            if (writer != null)
                try {
                    writer.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (fr != null) {
                try {
                    fr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (writer != null)
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    public DetailedListCdr packCdr(String cdrStr, String fileName) {
        StringBuffer value = new StringBuffer();
        StringBuffer rowKey = new StringBuffer();
        DetailedListCdr cdr;

        String[] str = cdrStr.split(CommonUtil.col_sep);

        /*
        value.append(str[0]).append("^");
        value.append(str[1]).append("^");
        String protocolId = CommonUtil.protocolIdCal(str[2]);
        value.append(protocolId).append("^");
        value.append(str[3]).append("^");
        value.append(str[4]).append("^");
        value.append(str[5]).append("^");
        String duration = CommonUtil.durationCal(str[6]);
        value.append(duration).append("^");
        value.append(str[7]).append("^");
        value.append(str[8]).append("^");
        value.append(str[9]).append("^");
        value.append(str[10]).append("^");

        value.append(str[11]).append("^");
        value.append(str[12]).append("^");
        value.append(str[13]);
        */
        rowKey.append(str[7]).append(str[2]); //.append(protocolId).append(duration);

        cdr = new DetailedListCdr();
        cdr.setRowKey(rowKey.toString());
        cdr.setValue(cdrStr);

        return cdr;
    }

    public class ReadFile implements Runnable {
        private File dir;
        private HBaseDao dao;

        public ReadFile(File dir, HBaseDao dao) {
            this.dir = dir;
            this.dao = dao;
        }

        public void run() {
            int time = 0;
            while (true) {
                File[] files = this.dir.listFiles();
                for (File file : files) {
                    if (file.isFile()) {
                        readFile(file, this.dao);
                        file.delete();
                    }
                }
                time++;
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                System.out.println(Thread.currentThread().getName() + ", " + format.format(new Date()) + ", time = " + time);
            }
        }
    }
}
