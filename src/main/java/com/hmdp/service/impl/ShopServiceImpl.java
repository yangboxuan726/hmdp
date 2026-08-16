package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
    @Override
    public Result queryById(Long id) {
        //解决缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //用封装工具类
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class,this::getById,CACHE_SHOP_TTL, TimeUnit.MINUTES);

        /*利用互斥锁解决缓存击穿*/
        //Shop shop = queryWithMutex(id);

        /*利用逻辑过期解决缓存击穿*/
        //Shop shop = queryWithLogicalExpire(id);
        //Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class,this::getById,20L, TimeUnit.SECONDS);

        if (shop == null){
            return Result.fail("店铺不存在!!!");
        }
        return Result.ok(shop);
    }
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    //缓存击穿使用逻辑过期
    /*public Shop queryWithLogicalExpire(Long  id){
        String Key = CACHE_SHOP_KEY + id;
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(Key);
        //2.判断是否存在
        if (StrUtil.isBlank(shopJson)){
            //3.未命中，直接返回
            return null;
        }

        //4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断逻辑过期
        if (expireTime.isAfter(LocalDateTime.now())){
            //5.1 未过期，直接返回店铺信息
            return shop;
        }

        //5.2 已过期，需要缓存重建
        //6.重建缓存
        //6.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //6.2 判断是否获取锁成功
        if (isLock){
            //6.3 成功，获取锁成功，开始独立线程重建缓存
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {

                    //重建缓存
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);

                } finally {

                    //释放锁
                    unLock(lockKey);
                }

            });

        }
        //6.4 返回过期店铺信息
        return shop;

    }*/

    //缓存击穿互斥锁
   /* public Shop queryWithMutex(Long id){
        String Key = CACHE_SHOP_KEY + id;
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(Key);
        //2.判断是否存在/命中
        if (StrUtil.isNotBlank(shopJson)){
            //3.命中，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);//先将字符串反序列化为Java对象
            return shop;
        }
        //判断命中是否是空值
        if (shopJson != null){
            return null;
        }
        //4.未命中，开始尝试缓存重建
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            //4.1 获取互斥锁
            boolean isLock = tryLock(lockKey);
            //4.2 判断是否获取锁成功
            //4.3 获取锁失败，休眠一会，然后重试
            if (!isLock){
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //4.4  获取锁成功，根据id查询数据库
            // ✅✅✅ 二次检查：获取锁成功后，再次检查 Redis 缓存是否存在
            shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id); // 重新查询
            if (StrUtil.isNotBlank(shopJson)) {
                // 如果此时发现缓存已经被其他线程写入，直接返回即可
                shop = JSONUtil.toBean(shopJson, Shop.class);
                return shop;
            }
            shop = getById(id);
            //模拟重建延时
            Thread.sleep(200);
            //5.数据库不存在，返回错误
            if (shop == null){
                //为了避免缓存穿透，我们把一个空值写入Redis
                stringRedisTemplate.opsForValue().set(Key , "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //6.存在，写入redis
            stringRedisTemplate.opsForValue().set(Key , JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //7.释放互斥锁
            unLock(lockKey);
        }

        //8.返回
        return shop;
    }*/

    //缓存穿透
    public Shop queryWithPassThrough(Long  id){
        String Key = CACHE_SHOP_KEY + id;
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(Key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)){
            //3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);//先将字符串反序列化为Java对象
            return shop;
        }
        //判断命中是否是空值
        if (shopJson != null){
            return null;
        }
        //4.不存在，根据id查询数据库
        Shop shop = getById(id);
        //5.数据库不存在，返回错误
        if (shop == null){
            //为了避免缓存穿透，我们把一个空值写入Redis
            stringRedisTemplate.opsForValue().set(Key , "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6.存在，写入redis
        stringRedisTemplate.opsForValue().set(Key , JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //7.返回
        return shop;

    }

    /*//获取锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1",10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //释放锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
        //1.查询店铺数据
        Shop shop = getById(id);
        //模拟重建延时
        Thread.sleep(200);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }*/


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);

        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY +id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if (x == null || y == null){
            //不需要坐标查询，按类型查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
        }
        //2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //3.查询redis、按照距离排序、分页
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key, GeoReference.fromCoordinate(x, y), new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance()
                                .limit(end)
                );
        //4.解析出id
        if (results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from){
            //没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        //4.1 截取
        List<Long> ids = new ArrayList<>(list.size());
        Map<String,Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip( from).forEach(result -> {
            //获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        //5.根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }
}
