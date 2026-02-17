package ru.yandex.practicum.delivery.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import ru.yandex.practicum.delivery.model.Delivery;
import ru.yandex.practicum.commerce.dto.DeliveryDto;

@Mapper(componentModel = "spring")
public interface DeliveryMapper {
    DeliveryMapper INSTANCE = Mappers.getMapper(DeliveryMapper.class);

    @Mapping(source = "fromCountry", target = "fromAddress.country")
    @Mapping(source = "fromCity", target = "fromAddress.city")
    @Mapping(source = "fromStreet", target = "fromAddress.street")
    @Mapping(source = "fromHouse", target = "fromAddress.house")
    @Mapping(source = "fromFlat", target = "fromAddress.flat")
    @Mapping(source = "toCountry", target = "toAddress.country")
    @Mapping(source = "toCity", target = "toAddress.city")
    @Mapping(source = "toStreet", target = "toAddress.street")
    @Mapping(source = "toHouse", target = "toAddress.house")
    @Mapping(source = "toFlat", target = "toAddress.flat")
    DeliveryDto toDto(Delivery delivery);

    @Mapping(source = "fromAddress.country", target = "fromCountry")
    @Mapping(source = "fromAddress.city", target = "fromCity")
    @Mapping(source = "fromAddress.street", target = "fromStreet")
    @Mapping(source = "fromAddress.house", target = "fromHouse")
    @Mapping(source = "fromAddress.flat", target = "fromFlat")
    @Mapping(source = "toAddress.country", target = "toCountry")
    @Mapping(source = "toAddress.city", target = "toCity")
    @Mapping(source = "toAddress.street", target = "toStreet")
    @Mapping(source = "toAddress.house", target = "toHouse")
    @Mapping(source = "toAddress.flat", target = "toFlat")
    Delivery toEntity(DeliveryDto deliveryDto);
}