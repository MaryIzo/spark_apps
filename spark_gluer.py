import sys
import logging
import pyspark
from typing import List, Dict, Callable

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)

class FileGraphGluing:
    """
    Class for gluing files in directory (folder). 
    
    Attributes
    ----------
    spark: spark context for app
    optimal_size (default 128): optimal hdfs file size
    files_format (default parquet): format of the files in directory to change (orc, parquet, etc.)
    debug (default False): flag to show logs of files (paths) while repartitioning
    
    graph_node: node of all files
    base_size: base size for converting from bytes into Megabytes
    coalesce_size: number of Mb in table for replacing repartition with coalesce
    
    FileSystem: hdfs file system (self.spark._jvm.org.apache.hadoop.fs.FileSystem)
    Path: hdfs java object path (self.spark._jvm.org.apache.hadoop.fs.Path)
    FileUtil: hdfs File Util (self.spark._jvm.org.apache.hadoop.fs.FileUtil)
    hadoop_conf: hdfs hadoop_conf (self.spark.sparkContext._jsc.hadoopConfiguration())
    fs: hdfs file system (self.FileSystem.get(self.hadoop_conf))
    
    Methods
    -------
    get_list_dir(path):
        lists directories in hdfs
    get_list_files(path):
        lists files in hdfs 
    gluing_for_file_path(string_path):
        makes gluing for string_path directory
    filegraph(current, last, visited, node):
        makes a node for files
    glue_directories_recursively(current, last):
        Main function for spark gluer
    """
    
    def __init__(
        self, 
        spark: pyspark.sql.session.SparkSession, 
        optimal_size: int = 128, 
        files_format: str = 'parquet', 
        debug: bool = False,
        coalesce_size: int = 100000
    ):
        
        self.spark = spark
        self.optimal_size = optimal_size
        self.format = files_format
        self.debug = debug
        self.graph_node = {}
        self.base_size = 1024
        self.coalesce_size = coalesce_size
    
        self.FileSystem = self.spark._jvm.org.apache.hadoop.fs.FileSystem
        self.Path = self.spark._jvm.org.apache.hadoop.fs.Path
        self.FileUtil = self.spark._jvm.org.apache.hadoop.fs.FileUtil
        self.hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        self.fs = self.FileSystem.get(self.hadoop_conf)
    
    def get_list_dir(
            self,
            path: 'spark._jvm.org.apache.hadoop.fs.Path'
    ) -> None:
        """
        Attributes
        -----------
        path: Path object for making a list (spark._jvm.org.apache.hadoop.fs.Path)
        """
        list_dir = []
        it = self.fs.listLocatedStatus(path)

        while it.hasNext():
            part = it.next()
            list_dir.append(part.getPath().toString())

        return list_dir
    
    def get_list_files(
            self,
            path: 'spark._jvm.org.apache.hadoop.fs.Path'
    ) -> None:
        """
        Attributes
        -----------
        path: Path object for making a list (spark._jvm.org.apache.hadoop.fs.Path)
        """
        logging.debug('Iterating over files: ')
        it = self.fs.listFiles(path, False)
        while it.hasNext():
            part = it.next()
            logging.info(part.getPath().toString())
            logging.info(part.getLen()/self.base_size/self.base_size)
        
    
    def gluing_for_file_path(
        self, 
        string_path: str
    ) -> None:
        """
        Gluing for directory string_path.
        
        Attributes:
        -------------
        string_path: string path for gluing files.
        """
        
        # Определяем объект первоначальной папки, в которой нужно склеить файлы
        first_path = self.Path(string_path)
        
        # Проверяем, что эта папка не содержит внутри себя папок, иначе не получится переместить
        if self.fs.getContentSummary(first_path).getDirectoryCount() > 1:
            logging.debug('Has another directories. Return.')
            return
        
        # Выводим текущее состояни папки, если в ней только файлы
        logging.debug('Before changing table: ')
        if self.debug:
            self.get_list_files(first_path)
            
        # Подсчитываем число файлов в директории с ориентацией на размер блока 128 Мб --- getLength() возвращает число байт -- переводим в Mb
        directory_size = self.fs.getContentSummary(first_path).getLength()
        directory_size = directory_size/self.base_size/self.base_size
        
        optimal_count = round(directory_size/self.optimal_size)
        optimal_count = 1 if optimal_count == 0 else optimal_count
        
        # Подсчитываем текущее число файлов в директории
        real_count = self.fs.getContentSummary(first_path).getFileCount()
        
        # Проверяем наличие файла SUCCESS, чтобы не учитывать его в расчёте
        try:
            if self.fs.exists(self.Path(f"{string_path}/_SUCCESS")):
                real_count -= 1
            
        except Exception as err:
            logging.debug(f"org.apache.hadoop.security.AccessControlException: Permission denied (in check): {string_path}/_SUCCESS")
        
        logging.debug(f'expected count: {optimal_count}')
        logging.debug(f'real count: {real_count}')
        
        # Сравниваем оптимальное и большое число мелких файлов.
        if optimal_count >= real_count:
            return 
        
        # Создаем временную папку для сохранения
        tmp_string_path = f"{string_path}-tmp"
        tmp_path = self.Path(tmp_string_path)
        if self.fs.exists(tmp_path):
            self.fs.delete(tmp_path)
            
        # пересохраняем
        df = self.spark.read.load(string_path, format=self.format) # чтение без спецификации формата
        
        if directory_size > self.coalesce_size:
            logging.info('Coalesce started ... ')
            df.coalesce(optimal_count).write.mode("overwrite").format('parquet').save(tmp_string_path)
        else:
            logging.info('Repartition started ... ')
            df.repartition(optimal_count).write.mode("overwrite").format('parquet').save(tmp_string_path)
        
        # Удаляем старые файлы в папке
        it = self.fs.listFiles(first_path, False)
        while it.hasNext():
            part = it.next()
            to_delete = self.Path(part.getPath().toString())
            self.fs.delete(to_delete)
            
        # Перемещаем содержание tmp
        it = self.fs.listFiles(tmp_path, False)
        while it.hasNext():
            part = it.next()
            to_move = self.Path(part.getPath().toString())
            self.FileUtil.copy(self.fs, to_move, self.fs, first_path, True, self.hadoop_conf)
            
        # Удаляем tmp папку
        self.fs.delete(tmp_path)
        
        # Выводим новое распределение файлов
        logging.debug('After repartition: ')
        if self.debug:
            self.get_list_files(first_path)
        
    def filegraph(
        self, 
        current: str, 
        last: str,
        visited: List[str],
        node: Dict[str, str]
    ) -> Callable[[str, str, List[str], Dict[str, str]], None]:
        """
        Graph for all folders in the node.
        Table can have a lot of partitions. 
        """
        
        if current:
            current_path = self.Path(current)
        else:
            return
        
        logging.debug(f'Current directory: {current}')
        logging.debug(f'Last directory: {last}')
        
        if current in visited:
            logging.debug(f'Visited: {visited}')
            for key in node:
                logging.debug(f'{key} -> {node[key]}')
            logging.debug(f'Visit +: {current}')
            return 
        else:
            names = self.get_list_dir(current_path) # Получаем список объектов в директории через файловую систему hdfs
            visited.append(current)

        current_dir_items = [] #это список папок в текущей директории
        
        if self.fs.getContentSummary(current_path).getDirectoryCount() > 1:
            for name in names:           
                current_dir_items.append(name)
                logging.debug(f'Directory: {name}')
        else:
            logging.info(f'Directory for gluing: {current}')  # лог обрабатываемой склеивателем папки в любом случае выводится
            self.gluing_for_file_path(current)

        logging.debug(f'List: {current_dir_items}')

        logging.debug(f'Last: {last} \n')
        node[current] = last, current_dir_items 

        if last in node:
            logging.debug(f'Last node: {node[last]}')
        else:
            logging.debug('No key')

        logging.debug(f'Current node: {node[current]}')
        if len(current_dir_items) != 0:

            for next_item in current_dir_items: 
                if next_item not in visited: 
                    for key in node:
                        logging.debug(f' {key}, {node[key]}')
                    logging.debug(f'{next_item} Current -------------- ')
                    self.filegraph(next_item, current, visited, node)  

        else:
            logging.debug('Returning')
            logging.debug(f'{last} Current  0')
            for key in node:
                logging.debug(f'{key} -> {node[key]}')

            return self.filegraph(last, '', visited, node)
        
    def glue_directories_recursively(
        self, 
        current: str, 
        last: str = ''
    ) -> None:
        """
        Main function for recursion in the folder
        """
        visited = []
        node = {}
        self.filegraph(current, last, visited, node)
        self.graph_node = node
