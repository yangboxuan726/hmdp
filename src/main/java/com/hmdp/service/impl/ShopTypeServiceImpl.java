package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPList_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result showType() {
        String Key = CACHE_SHOPList_KEY;
        //1.从redis查询商铺缓存
        String shopList = stringRedisTemplate.opsForValue().get(Key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopList)){
            //3.存在，直接返回
            //先将字符串反序列化为Java对象
            List<ShopType> shopTypeList = JSONUtil.toList(shopList, ShopType.class);
            return Result.ok(shopTypeList);
        }
        //4.不存在，查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();
        //5.数据库不存在，返回错误
        if (typeList == null){
            return Result.fail("未查找到商铺信息");
        }
        //6.存在，写入redis
        stringRedisTemplate.opsForValue().set(Key, JSONUtil.toJsonStr(typeList));
        //7.返回
        return Result.ok(typeList);
    }
}
