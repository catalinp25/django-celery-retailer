
class BuyTransactionViewset(BaseTransactionViewset):
    queryset = Order.objects.all()
    serializer_class = BuyTransactionSerializer

    @action(detail=True, methods=['patch'])
    def cancel_shipment(self, request, pk=None):
        obj = self.get_object()
        self.do_action(obj, CancelShipmentSerializer, request)
        return response.Response({'success': True})

    @action(detail=True, methods=['patch'])
    def receiver_decline_checkout(self, request, pk=None):
        obj = self.get_object()
        if request.data.get('reason') is None:
            raise exceptions.ValidationError(
                {"reason": "a reason is required"})
        try:
            if obj._state_not_completed(
                'receiver_approve_modified_purchase',
                obj.receiver,
            ).exists():
                obj.receiver_reject_modified_purchase(
                    request.data.get('reason'))
            else:
                obj.receiver_decline_checkout(request.data.get('reason'))
        except Exception as e:
            if settings.DEBUG:
                raise
            logger.error(
                'There was an error, with a stacktrace!', exc_info=True)
            raise exceptions.ValidationError(
                {"unknown": "something went wrong"})
        return response.Response({'success': True})

    @action(detail=True, methods=['patch'])
    def provider_decline(self, request, pk=None):
        obj = self.get_object()

        # Send email to provider to keep their inventory up-to date
        CommunicationSender().send(
            'DeclinedOrderConfirmation',
            recipients=obj.provider.get_contacts()
        )

        self.do_action(obj, DeclineSaleSerializer, request)
        return response.Response({'success': True})

    @transaction.atomic
    @action(detail=True, methods=['patch'])
    def modify_provider_contributions(self, request, pk=None):
        """
        We are in a situation where we already authorized the receivers payment method.
        Any changes by the provider need to fit within that dollar amount so we don't need
        to re-auth.
        """
        obj = self.get_object()

        if request.retailer.id not in [obj.provider.id]:
            raise exceptions.PermissionDenied(
                'You are not allowed to add or remove swap items.')

        ser = ModifySaleContributionSerializer(
            obj,
            request.data,
            context={
                'request': request,
                'original_receiver_total': obj.receiver_grand_total,
                'original_unit_checksum': obj.contribution_listing_item_quantity_checksum(retailer=obj.provider)
            })
        if ser.is_valid(raise_exception=True):
            ser.save()

        ser = self.get_serializer(self.get_object())
        return response.Response(ser.data)

    @action(detail=True, methods=['patch'])
    def approve(self, request, pk=None):
        raise exceptions.ValidationError({"need_payment_method": True})

    @transaction.atomic
    @action(detail=True, methods=['patch'])
    def modify_receiver_contributions(self, request, pk=None):
        """
        We are in a situation where we already authorized the receivers payment method.
        Any changes need to fit within that dollar amount so we don't need
        to re-auth.
        """
        obj = self.get_object()

        if request.retailer.id not in [obj.receiver.id]:
            raise exceptions.PermissionDenied(
                'You are not allowed to add or remove swap items.')

        ser = ModifySaleContributionSerializer(
            obj,
            request.data,
            context={
                'request': request,
                'original_receiver_total': obj.paymenttransaction_set.filter(name="receiver payment").first().amount,
                'original_unit_checksum': obj.contribution_listing_item_quantity_checksum(retailer=obj.provider)

            })
        if ser.is_valid(raise_exception=True):
            ser.save()

        ser = self.get_serializer(self.get_object())
        return response.Response(ser.data)

    @action(detail=True, methods=['patch'])
    def receiver_approve_modified_purchase(self, request, pk=None):
        obj = self.get_object()
        obj.receiver_approve_modified_purchase()
        return response.Response({'success': True})

    @action(detail=True, methods=['patch'])
    def receiver_reject_modified_purchase(self, request, pk=None):
        obj = self.get_object()
        if request.data.get('reason') is None:
            raise exceptions.ValidationError(
                {"reason": "a reason is required"})
        obj.receiver_reject_modified_purchase(request.data.get('reason'))
        return response.Response({'success': True})


class QuerysetMaker(object):
    transaction_type_filter = {}

    queryset = Transaction.objects.exclude(is_active=False)

    def get_ownership_filter(self):
        return [Q(receiver=self.request.retailer) | Q(provider=self.request.retailer)]

    def get_queryset(self):
        if not self.request.retailer:
            return Transaction.objects.none()

        filters = []

        if self.request.retailer:
            filters = [
                Q(sender=self.request.retailer) | Q(
                    receiver=self.request.retailer)
            ]
        elif not self.request.user.is_staff:
            raise exceptions.PermissionDenied

        shipment_qs = Shipment.objects.filter(
            *filters
        ).exclude(status__in=[DEAD_SHIPMENT, 'cancelled']).order_by('-modified').values_list(
            'transaction__id', flat=True
        )

        highest = TransactionAction.objects.filter(
            transaction=OuterRef('id'),
        ).order_by('-created')

        myhighest = TransactionAction.objects.filter(
            transaction=OuterRef('id'),
            participant=self.request.retailer.id,
            complete__isnull=True,
        )

        active = TransactionAction.objects.filter(
            transaction=OuterRef('id'),
            complete__isnull=True,
        )

        lastcomplete = TransactionAction.objects.filter(
            transaction=OuterRef('id'),
            complete=True,
        )

        transactions = self.queryset.filter(
            *self.get_ownership_filter(),
            **self.transaction_type_filter,
        ).exclude(
            Q(receiver_decline_checkout_reason__isnull=False) & Q(receiver=self.request.retailer) |
            Q(id__in=shipment_qs),
        ).annotate(
            complete=Subquery(
                myhighest.values('complete')[:1]
            ),
            action=Subquery(
                highest.values('state')[:1]
            ),
            action_id=Subquery(
                myhighest.values('id')[:1]
            ),
            seen_by_provider=Subquery(
                highest.values('seen_by_provider')[:1]
            ),
            seen_by_receiver=Subquery(
                highest.values('seen_by_receiver')[:1]
            ),
            participant=Subquery(
                highest.values('participant__id')[:1]
            ),
            myhigh=Subquery(
                myhighest.values('created')[:1]
            ),
            myhighstate=Subquery(
                myhighest.values('state')[:1]
            ),
            highactive=Subquery(
                active.values('state')[:1]
            ),
            lastcomplete=Subquery(
                lastcomplete.values('state')[:1]
            )
        ).order_by('-modified').select_related('brand').prefetch_related(
            Prefetch(
                'actions',
                queryset=TransactionAction.objects.filter(
                ),
                to_attr='last_actions'
            ),
            'transactioncontribution_set',
        )
        return transactions


class ActiveSellTransactionViewset(QuerysetMaker, BuyTransactionViewset):
    serializer_class = ActiveTransactionSerializer
    queryset = QuerysetMaker.queryset.all()
    transaction_type_filter = {"transaction_type": 1}

    def get_ownership_filter(self):
        return [Q(provider=self.request.retailer)]


class ActiveBuyTransactionViewset(QuerysetMaker, BuyTransactionViewset):
    serializer_class = ActiveTransactionSerializer
    queryset = QuerysetMaker.queryset.all()
    transaction_type_filter = {"transaction_type": 1}

    def get_ownership_filter(self):
        return [Q(receiver=self.request.retailer)]


class ActiveSwapTransactionViewset(QuerysetMaker, BuyTransactionViewset):
    serializer_class = ActiveTransactionSerializer
    queryset = QuerysetMaker.queryset.all()
    transaction_type_filter = {"transaction_type": 2}


class ActiveTransactionSummaryViewset(QuerysetMaker, SwapBaseViewSet):
    serializer_class = ActiveTransactionSerializer
    queryset = QuerysetMaker.queryset.filter()
    transaction_type_filter = {}

    @action(detail=False, methods=['get'])
    def alerts(self, request):

        data = {
            "purchase_alerts": 0,
            "sale_alerts": 0,
            "swap_alerts": 0,
        }

        SALE = 1
        SWAP = 2
        qs = self.get_queryset().filter(
            (Q(receiver=request.retailer) & Q(seen_by_receiver=False)) |
            (Q(provider=request.retailer) & Q(seen_by_provider=False)) |
            ((Q(receiver=request.retailer) | Q(provider=request.retailer))
             & Q(complete__isnull=True))
        )
        for rt in qs:
            IS_PROVIDER = rt.provider.id == request.retailer.id
            IS_RECEIVER = rt.receiver.id == request.retailer.id
            if rt.transaction_type == SALE and IS_RECEIVER:
                data['purchase_alerts'] += 1
            elif rt.transaction_type == SALE and IS_PROVIDER:
                data['sale_alerts'] += 1
            else:
                for ta in rt.last_actions:
                    if ta.participant.id == request.retailer.id and not ta.complete:
                        data['swap_alerts'] += 1
        return response.Response(data)


class TransactionActionPackingSlipViewset(SwapBaseViewSet):
    serializer_class = TransactionActionSerializer
    queryset = TransactionAction.objects.all()

    @action(detail=False, methods=['post'])
    def seen(self, request):
        qs = self.queryset.filter(
            transaction__id__in=request.data.get('provider', []),
            transaction__provider=request.retailer,
            seen_by_provider=False,
        )
        res = qs.update(
            seen_by_provider=True
        )

        qs = self.queryset.filter(
            transaction__id__in=request.data.get('receiver', []),
            transaction__receiver=request.retailer,
            seen_by_receiver=False,
        )

        res += qs.update(
            seen_by_receiver=True
        )

        # outputs the lesser of the number of IDs submitted
        # and the number of records updated.

        # if we updated 0 records, that's fine.
        # sometimes multiple records will update for each transaction,
        # we only want the number of transactions updated
        return response.Response(
            min(res, (
                len(request.data.get('provider', [])) +
                len(request.data.get('receiver', []))
            )
            )
        )

    @action(detail=False, methods=['get'])
    def gettransaction(self, request):
        # this view is callled with an external id

        transaction = Transaction.get_by_external_id(
            request.query_params.get('tx'))._get()
        if not request.user.is_staff:
            if not request.retailer.id in [
                transaction.provider.id,
                transaction.receiver.id
            ]:
                raise exceptions.PermissionDenied

        return response.Response(
            PackingSlipTransactionSerializer(transaction).data
        )


class OrdersFilter(filters.FilterSet):

    type = filters.CharFilter(method='filter_type')
    stage = filters.CharFilter(method='filter_stage')
    action = filters.CharFilter(method='filter_action')
    id = filters.CharFilter(method='filter_id')
    retailer = filters.NumberFilter(method="filter_retailer")

    class Meta:
        model = Transaction
        fields = ['type']

    def filter_type(self, queryset, name, value):
        if value.isdigit():
            return queryset.filter(transaction_type=value)
        if value == 'swap':
            return queryset.filter(transaction_type=2)
        if value == 'purchase':
            return queryset.filter(transaction_type=1, receiver=self.request.retailer)
        if value == 'sale':
            return queryset.filter(transaction_type=1, provider=self.request.retailer)
        if value == 'rma':
            return queryset.filter(transaction_type=3)

    def filter_id(self, queryset, name, value):
        obj_id = Transaction._convert_id(value)
        return queryset.filter(id=value)

    def filter_retailer(self, queryset, name, value):
        return queryset.filter(Q(provider__id=value) | Q(receiver__id=value))

    def filter_stage(self, queryset, name, value):
        filters = {
            'pending': QUERY_CODES.PENDING,
            'pre_shipping': QUERY_CODES.PENDING,
            'approved':  QUERY_CODES.APPROVED,
            'shipping':  QUERY_CODES.SHIPPED,
            'delivered': QUERY_CODES.DELIVERED,
            'complete': QUERY_CODES.COMPLETED,
            'declined': QUERY_CODES.DECLINED,
            'cancelled': QUERY_CODES.CANCELLED,
            'rmarequested': QUERY_CODES.RMA_REQUESTED,
            'rmaapproved': QUERY_CODES.RMA_APPROVED,
            'rmadeclined': QUERY_CODES.RMA_DENIED,
            'rmacompleted': QUERY_CODES.RMA_COMPLETED,
        }
        if lookup := filters.get(value):
            queryset = queryset.filter(active_state__state__in=lookup)
        return queryset

    def filter_action(self, queryset, name, value):
        return queryset.filter(
            id__in=TransactionAction.objects.filter(
                complete=None,
                state=value,
                active=True,
            ).values_list('transaction__id', flat=True)
        )


class OrdersViewset(SwapBaseViewSet):

    serializer_class = OrderSerializer
    queryset = Transaction.objects.all()
    filter_class = OrdersFilter
    search_fields = ('brand__name', )
    filter_backends = (DjangoFilterBackend,
                       CustomOrdersSearchFilter, rest_filters.OrderingFilter)

    def get_queryset(self):

        qs = self.queryset.filter(
            *self.get_prefilter()
        ).exclude(
            is_active=False,
        ).prefetch_related(
            *self.get_prefetch()
        ).annotate(
            num_shipments=Count('shipment',  filter=~Q(
                shipment__status=DEAD_SHIPMENT)),
            num_delivered=Count('shipment', filter=Q(
                shipment__status='delivered')),
            num_in_route=Count('shipment', filter=Q(
                shipment__status='in-route'))
        ).select_related(
            'provider',
            'receiver',
            'brand',
        ).order_by('-modified')
        return qs

    def get_prefetch(self):
        out = [
            Prefetch(
                'actions',
                queryset=TransactionAction.objects.filter(
                ).select_related('transaction'),
                to_attr='last_actions'
            ),
            Prefetch(
                'shipment_set',
                to_attr='shipments',
                queryset=Shipment.objects.exclude(
                    status=DEAD_SHIPMENT).prefetch_related(Prefetch('shipmentevent_set'))
            ),
            Prefetch(
                'paymenttransaction_set',
                queryset=PaymentTransaction.objects.filter(
                ).select_related('transaction', 'payment_method').order_by('-modified'),
                to_attr='paymenttransactions'
            ),
            Prefetch(
                'transactioncontribution_set',
                to_attr='provider_contributions',
                queryset=TransactionContribution.objects.all().prefetch_related(
                    Prefetch('rmacontributionpicture_set', to_attr='pictures'),
                    Prefetch(
                        'listing_unit__listing__productvariation__product__productsize_set',
                        queryset=ProductSize.objects.select_related(
                            'size',
                            'size__label'
                        ).filter(
                            product_id=OuterRef('listing_unit__listing__productvariation__product_id')
                        ),
                        to_attr='product_sizes',
                    ),
                ).select_related('listing_unit__listing')
            ),
            Prefetch(
                'RMA_set',
                to_attr='RMAs',
                queryset=Transaction.objects.exclude(
                    is_active=False,
                )
            )
        ]

        return out

    def get_prefilter(self):

        filters = []

        if self.request.retailer:
            filters = [
                Q(provider=self.request.retailer) | Q(
                    receiver=self.request.retailer)
            ]
        elif not self.request.user.is_staff:
            raise exceptions.PermissionDenied

        return filters

    @action(detail=True, methods=['patch'])
    def rate(self, request, pk=None):

        obj = self.get_object()
        ser = {
            'provider': ProviderRatingSerializer,
            'receiver': ReceiverRatingSerializer
        }[obj.get_participant_role(self.request.retailer)](
            obj, request.data, context={'request': request}
        )
        if ser.is_valid(raise_exception=True):
            ser.save()

        return response.Response(ser.data)

    @action(detail=True, methods=['patch'])
    def requestRMA(self, request, pk=None):

        obj = self.get_object()
        ser = {
            # 'provider': ProviderRequestRMASerializer,
            # 'receiver': ReceiverRequestRMASerializer
        }[obj.get_participant_role(self.request.retailer)](
            obj, request.data, context={'request': request}
        )
        if ser.is_valid(raise_exception=True):
            ser.save()

        return response.Response(ser.data)

    @action(detail=True, methods=['get'])
    def history(self, request, pk=None):

        obj = self.get_object()
        ser = OrderHistorySerializer(
            obj.actions.order_by('created'), many=True)
        return response.Response(ser.data)

    @action(detail=False, methods=['get'])
    def summary(self, request):
        retailer = request.retailer
        if retailer:

            respsonse_obj = {}

            for txtype, kwargs in [
                ('sales', dict(provider=retailer)),
                ('purchases',  dict(receiver=retailer))
            ]:

                qs = self.get_queryset().filter(
                    active_state__isnull=False,
                    **kwargs
                ).order_by(
                    'status'
                ).values(
                    'status'
                ).annotate(
                    count=Count('status')
                )
                respsonse_obj[txtype] = {x["status"]: x['count'] for x in qs}

            respsonse_obj['metrics'] = RetailerOrderMetricsSerializer(
                retailer).data
            return response.Response(respsonse_obj)
        else:
            raise exceptions.PermissionDenied


class OrdersFeedViewSet(SwapBaseViewSet):
    serializer_class = OrdersFeedSerializer
    queryset = Transaction.objects.all()
    filter_class = OrdersFilter
    search_fields = ('brand__name',)
    filter_backends = (DjangoFilterBackend,
                       CustomOrdersSearchFilter, rest_filters.OrderingFilter)
    http_method_names = ['get', ]

    def get_queryset(self):
        limit = int(self.request.query_params.get(
            'limit') or settings.LIMIT_ORDERS_FEED)

        if self.request.retailer:
            ListingUnit = apps.get_model(
                'listings.ListingUnit'
            )
            return self.queryset.filter(is_active=True).exclude(
                provider=self.request.retailer,
                status=tx_status_codes.APPROVED
            ).select_related(
                'brand',
                'provider',
            ).annotate(
                category=Subquery(
                    ListingUnit.objects.filter(
                        transactioncontribution__transaction_id=OuterRef('id'),
                    ).annotate(
                        category=F(
                            'listing__productvariation__product__category__name')
                    ).values('category')[:1]
                ),
                parent_category=Subquery(
                    ListingUnit.objects.filter(
                        transactioncontribution__transaction_id=OuterRef('id'),
                    ).annotate(
                        category=F(
                            'listing__productvariation__product__category__parent__name')
                    ).values('category')[:1]
                ),
                image=Subquery(
                    ListingUnit.objects.filter(
                        transactioncontribution__transaction_id=OuterRef('id'),
                    ).select_related(
                        'listing',
                        'listing__productvariation'
                    ).annotate(
                        image=Subquery(
                            ProductVariationImage.objects.filter(
                                variation_id=OuterRef(
                                    'listing__productvariation_id')
                            ).annotate(actual=F('image__image')).values('actual')[:1]
                        )
                    ).values('image')[:1]
                ),
            ).order_by('-modified')[:limit]
        else:
            raise exceptions.PermissionDenied

    def list(self, request, *args, **kwargs):

        return response.Response(
            {'data': OrdersFeedSerializer(self.get_queryset(), many=True).data}
        )


class PackingSlipOrdersViewset(OrdersViewset):

    serializer_class = PackingSlipTransactionSerializer

    def get_object(self):
        """
        Returns the object the view is displaying.
        You may want to override this if you need to provide non-standard
        queryset lookups.  Eg if objects are referenced using multiple
        keyword arguments in the url conf.
        """
        queryset = self.filter_queryset(self.get_queryset())

        # Perform the lookup filtering.
        lookup_url_kwarg = self.lookup_url_kwarg or self.lookup_field

        assert lookup_url_kwarg in self.kwargs, (
            'Expected view %s to be called with a URL keyword argument '
            'named "%s". Fix your URL conf, or set the `.lookup_field` '
            'attribute on the view correctly.' %
            (self.__class__.__name__, lookup_url_kwarg)
        )

        filter_kwargs = {self.lookup_field: self.kwargs[lookup_url_kwarg]}
        obj = Transaction.get_by_external_id(self.kwargs[lookup_url_kwarg])

        if obj:
            obj = obj._get()
        else:
            raise exceptions.NotFound
        # May raise a permission denied
        self.check_object_permissions(self.request, obj)

        return obj

