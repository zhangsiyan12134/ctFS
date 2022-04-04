/********************************
 * 
 * 
 * 
 *******************************/

#include "ctfs_runtime.h"

#ifdef CTFS_DEBUG
uint64_t ctfs_debug_temp;
#endif

ct_runtime_t ct_rt;


struct timespec stopwatch_start;
struct timespec stopwatch_stop;

void timer_start(){
	clock_gettime(CLOCK_MONOTONIC, &stopwatch_start);
}

uint64_t timer_end(){
	clock_gettime(CLOCK_MONOTONIC, &stopwatch_stop);
	return calc_diff(stopwatch_start, stopwatch_stop);
}


ct_runtime_t* get_rt(){
    return &ct_rt;
}

void ct_time_stamp(struct timespec * time){
    clock_gettime(CLOCK_REALTIME, time);
}

int ct_time_greater(struct timespec * time1, struct timespec * time2){
    if(time1->tv_sec > time2->tv_sec){
        return 1;
    }
    else if(time1->tv_sec < time2->tv_sec){
        return 0;
    }
    else if(time1->tv_nsec > time2->tv_nsec){
        return 1;
    }
    else
        return 0;
}

char* enum_to_string(int mode){
    switch(mode){
        case O_RDONLY:
            return "O_RDONLY";
        case O_WRONLY:
            return "O_WRONLY";
        case O_RDWR:
            return "O_RDWR";
        default:
            return "UNKNOWN";
    }
}

void ctfs_lock_add_blocking(ct_fl_t *current, ct_fl_t *node){
    /* add the conflicted node into the head of the blocking list of the current node*/
    ct_fl_seg *temp;
    temp = (ct_fl_seg*)malloc(sizeof(ct_fl_seg));
    temp->prev = NULL;
    temp->next = current->fl_block;
    temp->addr = node;

    if(current->fl_block != NULL){
        current->fl_block->prev = temp;
    }
    current->fl_block = temp;
}

void ctfs_lock_add_waiting(ct_fl_t *current, ct_fl_t *node){
    /*add the current node to the wait list head of the conflicted node*/
    ct_fl_seg *temp;
    temp = (ct_fl_seg*)malloc(sizeof(ct_fl_seg));
    temp->prev = NULL;
    temp->next = node->fl_wait;
    temp->addr = current;
    if(node->fl_wait != NULL){
        node->fl_wait->prev = temp;
    }
    node->fl_wait = temp;
}

void ctfs_lock_remove_blocking(ct_fl_t *current){
    /* remove the current node from others' blocking list*/
    assert(current != NULL);
    ct_fl_seg *temp, *temp1, *prev, *next;
    temp = current->fl_wait;
    while(temp != NULL){    //go through all node this is waiting for current node
        temp1 = temp->addr->fl_block;
        while(temp1 != NULL){   //go thorough the blocking list on other nodes to find itself
            //compare range and mode
            //if((temp1->addr->fl_start == current->fl_start) && (temp1->addr->fl_end == current->fl_end) && (temp1->addr->fl_type == current->fl_type)){
            //or simply compares the address
            if(temp1->addr == current){
                prev = temp1->prev;
                next = temp1->next;
                if(prev != NULL)
                    prev->next = next;
                else
                    temp->addr->fl_block = NULL;    //last one in the blocking list
                if(next != NULL)
                    next->prev = prev;
                free(temp1);
                break;
            }
            temp1 = temp1->next;
        }
        //printf("\tNode %p removed from Node %p blocking list\n", current, temp);
        if(temp->next == NULL){ //last member in waiting list
            free(temp);
            current->fl_wait = NULL;
            break;
        }
        temp = temp->next;
        if(temp != NULL)    //if not the last member
            free(temp->prev);
    }
}

int check_overlap(struct ct_fl_t *node1, struct ct_fl_t *node2){
    /* check if two given range have conflicts */
    return ((node1->fl_start <= node2->fl_start) && (node1->fl_end >= node2->fl_start)) ||\
    ((node2->fl_start <= node1->fl_start) && (node2->fl_end >= node1->fl_start));
}

int check_access_conflict(struct ct_fl_t *node1, struct ct_fl_t *node2){
    /* check if two given file access mode have conflicts */
    return !((node1->fl_type == O_RDONLY) && (node2->fl_type == O_RDONLY));
}

ct_fl_t* ctfs_lock_list_add_node(int fd, off_t start, size_t n, int flag){
    /* add a new node to the lock list upon the request(combined into lock_acq below) */
    ct_fl_t *temp, *tail, *last;
    temp = (ct_fl_t*)malloc(sizeof(ct_fl_t));
    temp->fl_next = NULL;
    temp->fl_prev = NULL;
    temp->fl_block = NULL;
    temp->fl_wait = NULL;
    temp->fl_type = flag;
    temp->fl_fd = fd;
    temp->fl_start = start;
    temp->fl_end = start + n - 1;
    temp->node_id = temp;

    pthread_mutex_lock(&ct_rt.fl_lock[fd]);

    if(ct_rt.fl[fd] != NULL){
        tail = ct_rt.fl[fd];   //get the head of the lock list
        while(tail != NULL){
            //check if current list contains a lock that is not compatable
            if(check_overlap(tail, temp) && check_access_conflict(tail, temp)){
                ctfs_lock_add_blocking(temp, tail); //add the conflicted lock into blocking list
                //printf("\tNode %p is blocking the Node %p\n", tail, temp);
                ctfs_lock_add_waiting(temp, tail); //add the new node to the waiting list of the conflicted node
                //printf("\tNode %p is waiting the Node %p\n", temp, tail);
            }
            last = tail;
            tail = tail->fl_next;
        }
        temp->fl_prev = last;
        last->fl_next = temp;
    } else {
        ct_rt.fl[fd] = temp;
    }
    //printf("Node %p added, Range: %u - %u, mode: %s\n", temp, temp->fl_start, temp->fl_end, enum_to_string(temp->fl_type));
    pthread_mutex_unlock(&ct_rt.fl_lock[fd]);

    return temp;
}

void ctfs_lock_list_remove_node(int fd, ct_fl_t *node){
    /* remove a node from the lock list upon the request */
    assert(node != NULL);
    ct_fl_t *prev, *next;

    pthread_mutex_lock(&ct_rt.fl_lock[fd]);
    prev = node->fl_prev;
    next = node->fl_next;
    if (prev == NULL){
        if(next == NULL)
            ct_rt.fl[fd] = NULL;    //last one member in the lock list;
        else{
            ct_rt.fl[fd] = next;    //delete the very first node in list
            next->fl_prev = NULL;
        }
    } else {
        prev->fl_next = next;
        if (next != NULL)
            next->fl_prev = prev;
    }
    ctfs_lock_remove_blocking(node);
    //printf("Node %p removed, Range: %u - %u, mode: %s\n", node, node->fl_start, node->fl_end, enum_to_string(node->fl_type));
    pthread_mutex_unlock(&ct_rt.fl_lock[fd]);

    free(node);
}

void ctfs_lock_list_init(){
    for (int i = 0; i < CT_MAX_FD; i++){
        ct_rt.fl[i] = NULL;
    }
}

ct_fl_t*  __attribute__((optimize("O0"))) ctfs_rlock_lock(int fd, off_t offset, size_t count, int flag){
    ct_fl_t *node = ctfs_lock_list_add_node(fd, offset, count, flag);
	while(node->fl_block != NULL){} //wait for blocker finshed
    return node;
}

void ctfs_rlock_unlock(int fd, ct_fl_t *node){
    ctfs_lock_list_remove_node(fd, node);
}

