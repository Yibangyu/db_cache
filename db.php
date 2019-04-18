<?php
/**
 * @Author yibangyu
 * @describe 一个小型数据库的实现
*/
define('DB_STORE','.');

define('DB_BUCKET_SIZE', 262144);
define('DB_KEY_SIZE',128);
define('DB_INDEX_SIZE',DB_KEY_SIZE + 12);

define('DB_KEY_EXISTS', 1);
define('DB_FAILURE', -1);
define('DB_SUCCESS', 0);
class DB{
  private $idx_fp;
  private $dat_fp;
  private $closed;
  public function __construct(){
    if(!is_dir(DB_STORE)){
      mkdir(DB_STORE);
    }
  }
  public function open($pathname){
    $idx_path = DB_STORE.'/'.$pathname.'.idx';
    $dat_path = DB_STORE.'/'.$pathname.'.dat';
    if(!file_exists($idx_path)){
      $init = true;
      $mod = "w+b";
    }else{
      $init = false;
      $mod = "r+b";
    }
    $this->idx_fp = fopen($idx_path, $mod);
    if(!$this->idx_fp){
      throw new Exception('Database file creation failed');
      return DB_FAILURE;
    }
    # 初始化索引文件，写入262144个long类型的0进去，每个占4个字节
    if($init){
      $elem = pack('L', 0x00000000);
      for($i=0;$i<DB_BUCKET_SIZE;$i++){
        fwrite($this->idx_fp, $elem, 4);
      }
    }
    $this->dat_fp = fopen($dat_path, $mod);
    if(!$this->dat_fp){
      throw new Exception('Database file creation failed');
      return DB_FAILURE;
    }
    return DB_SUCCESS;
  }
  public function _hash($key){
    $string = substr(md5($key), 0, 8);
    $hash = 0;
    for($i=0; $i<8; $i++){
      $hash += 33*$hash + ord($string[$i]);
    }
    return $hash & 0x7FFFFFFF;
  }
  public function get($key){
    $offset = ($this->_hash($key) % DB_BUCKET_SIZE)*4;
    fseek($this->idx_fp, $offset, SEEK_SET);
    $pos = unpack('L',fread($this->idx_fp, 4));
    $pos = $pos[1];
    $found = false;
    while($pos){
      fseek($this->idx_fp, $pos, SEEK_SET);
      $block = fread($this->idx_fp, DB_INDEX_SIZE);
      $cpkey = substr($block, 4, DB_KEY_SIZE);

      if(!strncmp($key, $cpkey, strlen($key))){
        $dataoff = unpack('L', substr($block, DB_KEY_SIZE+4, 4));
        $dataoff = $dataoff[1];

        $datalen = unpack('L', substr($block, DB_KEY_SIZE+8, 4));
        $datalen = $datalen[1];
        $found = true;
        break;
      }
      $pos = unpack('L', substr($block, 0, 4));
      $pos = $pos[1];
    }
    if(!$found){
      return NULL;
    }
    fseek($this->dat_fp, $dataoff, SEEK_SET);
    $data = fread($this->dat_fp, $datalen);
    return $data;
  }
  public function set($key, $data){
    $offset = ($this->_hash($key) % DB_BUCKET_SIZE)*4;

    $idxoff = fstat($this->idx_fp);
    $idxoff = intval($idxoff['size']);

    $datoff = fstat($this->dat_fp);
    $datoff = intval($datoff['size']);

    $keylen = strlen($key);
    if($keylen > DB_KEY_SIZE){
      throw new Exception('Key size must be less than '.DB_KEY_SIZE.' bytes');
      return DB_FAILURE;
    }

    $block = pack('L', 0x00000000);
    $block .= $key;
    $space = DB_KEY_SIZE - $keylen;
    for($i=0; $i<$space; $i++){
      $block .= pack('C',0x00);
    }
    $block .= pack('L', $datoff);
    $block .= pack('L', strlen($data));

    fseek($this->idx_fp, $offset, SEEK_SET);
    $pos = unpack('L',fread($this->idx_fp, 4));
    $pos = $pos[1];

    # 该hash还没有数据，直接插入
    if($pos == 0){
      fseek($this->idx_fp, $offset, SEEK_SET);
      fwrite($this->idx_fp, pack('L',$idxoff), 4);

      fseek($this->idx_fp, 0, SEEK_END);
      fwrite($this->idx_fp, $block, DB_INDEX_SIZE);
      fseek($this->dat_fp, 0, SEEK_END);
      fwrite($this->dat_fp, $data, strlen($data));

      return DB_SUCCESS;
    }
    while($pos){
      fseek($this->idx_fp, $pos, SEEK_SET);
      $tmp_block = fread($this->idx_fp, DB_INDEX_SIZE);
      $cpkey = substr($tmp_block, 4, DB_KEY_SIZE);
      if(!strncmp($key, $cpkey,strlen($key))){
        fseek($this->idx_fp, $pos+DB_KEY_SIZE+4, SEEK_SET);
        fwrite($this->idx_fp, substr($block,DB_KEY_SIZE+4,8), 8);

        fseek($this->dat_fp, 0, SEEK_END);
        fwrite($this->dat_fp, $data, strlen($data));
        return DB_SUCCESS;
      }
    $prev = $pos;
    $pos = unpack('L', substr($tmp_block, 0, 4));
    $pos = $pos[1];
    }
    fseek($this->idx_fp, $prev, SEEK_SET);
    fwrite($this->idx_fp, pack('L', $idxoff), 4);
    fseek($this->idx_fp, 0, SEEK_END);
    fwrite($this->idx_fp, $block, DB_INDEX_SIZE);
    fseek($this->dat_fp, 0, SEEK_END);
    fwrite($this->dat_fp, $data, strlen($data));
    return DB_SUCCESS;
  }

  public function delete($key){
    $offset = ($this->_hash($key) % DB_BUCKET_SIZE)*4;
    fseek($this->idx_fp, $offset, SEEK_SET);
    $pos = unpack('L', fread($this->idx_fp,4));
    $pos = $pos[1];
    $prev = $offset;
    $found = false;
    while($pos){
      fseek($this->idx_fp, $pos, SEEK_SET);
      $block = fread($this->idx_fp, DB_INDEX_SIZE);
      $next = unpack('L', substr($block, 0, 4));
      $next = $next[1];
      $cpkey = substr($block, 4, DB_KEY_SIZE);
      if(!strncmp($key, $cpkey, strlen($key))){
        fseek($this->idx_fp, $prev, SEEK_SET);
        fwrite($this->idx_fp, pack('L',$next), 4);
        $found = true;
        break;
      }
      $prev = $pos;
      $pos = $next;
    }
    if(!$found){
      return DB_FAILURE;
    }
    return DB_SUCCESS;
  }
  public function close(){
    if(!$this->closed){
      fclose($this->idx_fp);
      fclose($this->dat_fp);
      $this->closed = true;
    }
  }
}
