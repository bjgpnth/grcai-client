case $1 in

    "host")
        export SANITY_ENV=az-host-services
        ;;
    "docker")
        export SANITY_ENV=az-docker-services
        ;;
    "qa")
        export SANITY_ENV=qa
        ;;
    *)
        echo "Invalid environment"
        exit 1
esac

./sanity/sanity_run_all.sh

